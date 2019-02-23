package io.pravega.benchmark.analysis;

import io.pravega.benchmark.analysis.ioformat.CsvInputFormat;
import io.pravega.benchmark.loadtest.reports.Stats;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.net.URI;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class MeasureStats {

    public static FlinkPravegaReader<Stats> getFlinkPravegaReader(Stream stream, String controllerUri) {

        URI controller = URI.create(controllerUri);
        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withDefaultScope(stream.getScope())
                .withControllerURI(controller);

        FlinkPravegaReader<Stats> flinkPravegaReader = FlinkPravegaReader.<Stats>builder()
                .withPravegaConfig(pravegaConfig)
                .forStream(stream)
                .withDeserializationSchema(PravegaSerialization.deserializationFor(Stats.class))
                .build();
        return flinkPravegaReader;
    }

    public static WindowStats process(String window, Iterable<InputData> elements) {
        long count = 0;
        long bytes = 0;
        long eventSize = 0;
        List<Long> latencies = new ArrayList<>();
        for (InputData inputData: elements) {
            count++;
            bytes+= inputData.getEventSize();
            latencies.add(inputData.getLatency());
            eventSize = inputData.getEventSize();
        }

        double bytesMB = ((double) bytes / (1024*1024));
        String bytesInMB = String.format("%.2f", bytesMB);
        Latency latency = Latency.getLatency(latencies);

        WindowStats windowStats = new WindowStats(window, eventSize, count, bytes, bytesInMB, latency);
        return windowStats;
    }

    // find the throughput and latency for the entire run
    public void getThroughputAndLatencyForTheEntireRunBatch(String csvFile, String mode) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setTaskCancellationTimeout(60000);

        try {
            CsvInputFormat inputFormat = new CsvInputFormat(csvFile);
            DataSet<InputData> dataSet = env.createInput(inputFormat);
            List<InputData> inputDataList = dataSet.collect();
            int size = inputDataList.size();
            if (size <= 0) {
                log.info("No results found from the csv file");
                return;
            }

            Instant startTime = null;
            Instant endTime = null;
            List<Long> latencies = new ArrayList<>();
            long totalBytes = 0;
            int count = 0;

            for (InputData inputData: inputDataList) {
                if (!inputData.getRunMode().equals(mode)) continue;
                if (count == 0) {
                    startTime = inputData.getStartTime();
                } else {
                    endTime = inputData.getEndTime();
                }
                latencies.add(inputData.getLatency());
                totalBytes += inputData.getEventSize();
                count++;
            }

            long elapsedTimeInSeconds = Duration.between(startTime, endTime).toMillis() / 1000;
            long eventSize = inputDataList.get(0).getEventSize();
            String window = "duration :" + elapsedTimeInSeconds + " seconds";
            double bytesMB = ((double) totalBytes / (1024*1024) / elapsedTimeInSeconds);
            String bytesInMB = String.format("%.2f %s", bytesMB, "MB/s");
            Latency latency = Latency.getLatency(latencies);
            WindowStats windowStats = new WindowStats(window, eventSize, count, totalBytes, bytesInMB, latency);
            log.info("{}", windowStats);

        } catch (Exception e) {
            log.error("unable to calculate the stats", e);
        }
    }

    // find the throughput and latency for the entire run using streaming
    public void getThroughputAndLatencyForTheEntireRunStreaming(String csvFile, long totalRunIntervalInSeconds, String mode) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<InputData> source = env.addSource(new InputDataSource(csvFile));

        DataStream<WindowStats> statsDataStream =
        source.assignTimestampsAndWatermarks(new ExtractStartTimeBoundedOutOfOrderness(Time.seconds(0)))
                .filter((FilterFunction<InputData>) value -> value.getRunMode().equals(mode))
                .keyBy(e -> e.getEventSize())
                .timeWindow(Time.seconds(totalRunIntervalInSeconds))
                .aggregate(new AggregateFunction<InputData, AggregatedThroughputLatency, WindowStats>() {
                    @Override
                    public AggregatedThroughputLatency createAccumulator() {
                        return new AggregatedThroughputLatency();
                    }

                    @Override
                    public AggregatedThroughputLatency add(InputData inputData, AggregatedThroughputLatency accumulator) {
                        if (accumulator.getCount() == 0) {
                            accumulator.eventSize = inputData.getEventSize();
                            accumulator.startTime = inputData.getStartTime();
                        } else {
                            accumulator.endTime = inputData.getEndTime();
                        }
                        accumulator.latencies.add(inputData.getLatency());
                        accumulator.totalBytes += inputData.getEventSize();
                        accumulator.count++;

                        return accumulator;
                    }

                    @Override
                    public WindowStats getResult(AggregatedThroughputLatency accumulator) {
                        return accumulator.calculate();
                    }

                    @Override
                    public AggregatedThroughputLatency merge(AggregatedThroughputLatency a, AggregatedThroughputLatency b) {
                        AggregatedThroughputLatency aggregatedThroughputLatency = new AggregatedThroughputLatency();
                        aggregatedThroughputLatency.eventSize = a.eventSize;
                        aggregatedThroughputLatency.startTime = a.startTime;
                        aggregatedThroughputLatency.endTime = a.endTime;

                        aggregatedThroughputLatency.count = a.count + b.count;
                        aggregatedThroughputLatency.latencies.addAll(a.latencies);
                        aggregatedThroughputLatency.latencies.addAll(b.latencies);
                        aggregatedThroughputLatency.totalBytes = a.totalBytes + b.totalBytes;
                        return aggregatedThroughputLatency;
                    }
                });

        statsDataStream.print();

        try {
            env.execute("getThroughputAndLatencyForTheEntireRunStreaming");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    // find the throughput and latency for the entire run using streaming
    public void getThroughputAndLatencyForTheEntireRunStreamingFromPravega(FlinkPravegaReader<Stats> flinkPravegaReader, long totalRunIntervalInSeconds, String mode) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<Stats> source = env.addSource(flinkPravegaReader);

        DataStream<WindowStats> statsDataStream =
                source.assignTimestampsAndWatermarks(new ExtractStartTimeBoundedOutOfOrdernessPravega(Time.seconds(0)))
                        .filter((FilterFunction<Stats>) value -> value.getRunMode().name().equals(mode))
                        .keyBy(e -> e.getEventSize())
                        .timeWindow(Time.seconds(totalRunIntervalInSeconds))
                        .aggregate(new AggregateFunction<Stats, AggregatedThroughputLatency, WindowStats>() {
                            @Override
                            public AggregatedThroughputLatency createAccumulator() {
                                return new AggregatedThroughputLatency();
                            }

                            @Override
                            public AggregatedThroughputLatency add(Stats inputData, AggregatedThroughputLatency accumulator) {
                                if (accumulator.getCount() == 0) {
                                    accumulator.eventSize = inputData.getEventSize();
                                    accumulator.startTime = inputData.getStartTime();
                                } else {
                                    accumulator.endTime = inputData.getEndTime();
                                }
                                accumulator.latencies.add(inputData.getLatency());
                                accumulator.totalBytes += inputData.getEventSize();
                                accumulator.count++;

                                return accumulator;
                            }

                            @Override
                            public WindowStats getResult(AggregatedThroughputLatency accumulator) {
                                return accumulator.calculate();
                            }

                            @Override
                            public AggregatedThroughputLatency merge(AggregatedThroughputLatency a, AggregatedThroughputLatency b) {
                                AggregatedThroughputLatency aggregatedThroughputLatency = new AggregatedThroughputLatency();
                                aggregatedThroughputLatency.eventSize = a.eventSize;
                                aggregatedThroughputLatency.startTime = a.startTime;
                                aggregatedThroughputLatency.endTime = a.endTime;

                                aggregatedThroughputLatency.count = a.count + b.count;
                                aggregatedThroughputLatency.latencies.addAll(a.latencies);
                                aggregatedThroughputLatency.latencies.addAll(b.latencies);
                                aggregatedThroughputLatency.totalBytes = a.totalBytes + b.totalBytes;
                                return aggregatedThroughputLatency;
                            }
                        });

        statsDataStream.print();

        try {
            env.execute("getThroughputAndLatencyForTheEntireRunStreaming");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // find the response throughput and latency for a window interval using non-keyed stream
    public void calcStatsForFixedWindowIntervalNonKeyed(String csvFile, long windowIntervalInSeconds, String mode) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<InputData> source = env.addSource(new InputDataSource(csvFile));

        source.assignTimestampsAndWatermarks(new ExtractEndTimeBoundedOutOfOrderness(Time.seconds(windowIntervalInSeconds * 5)))
                .filter((FilterFunction<InputData>) value -> value.getRunMode().equals(mode))
                .timeWindowAll(Time.seconds(windowIntervalInSeconds))
                .process(new ProcessAllWindow())
                .print();

        try {
            env.execute("calcStatsForFixedWindowIntervalNonKeyed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // find the response throughput and latency for a window interval using keyed stream
    public void calcStatsForFixedWindowInterval(String csvFile, long windowIntervalInSeconds, String mode) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<InputData> source = env.addSource(new InputDataSource(csvFile));

        source.assignTimestampsAndWatermarks(new ExtractEndTimeBoundedOutOfOrderness(Time.seconds(windowIntervalInSeconds * 5)))
                .filter((FilterFunction<InputData>) value -> value.getRunMode().equals(mode))
                .keyBy(e -> e.getEventSize())
                .timeWindow(Time.seconds(windowIntervalInSeconds))
                .process(new ProcessWindow())
                .print();

        try {
            env.execute("calcStatsForFixedWindowInterval");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // find the response throughput and latency for a window interval and a slide using keyed stream
    public void calcStatsForSlidingWindowInterval(String csvFile, long windowIntervalInSeconds, long windowSlideIntervalInSeconds, String mode) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<InputData> source = env.addSource(new InputDataSource(csvFile));

        source.assignTimestampsAndWatermarks(new ExtractEndTimeBoundedOutOfOrderness(Time.seconds(windowIntervalInSeconds * 5)))
                .filter((FilterFunction<InputData>) value -> value.getRunMode().equals(mode))
                .keyBy(e -> e.getEventSize())
                .timeWindow(Time.seconds(windowIntervalInSeconds), Time.seconds(windowSlideIntervalInSeconds))
                .process(new ProcessWindow())
                .print();

        try {
            env.execute("calcStatsForSlidingWindowInterval");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // find the request rate stats using start time as event time
    public void calcStatsForRequestRate(String csvFile, long windowIntervalInSeconds, String mode, long entireRunWindowTimeInSeconds) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<InputData> source = env.addSource(new InputDataSource(csvFile));

        DataStream<WindowStats> statsDataStream =
        source.assignTimestampsAndWatermarks(new ExtractStartTimeBoundedOutOfOrderness(Time.seconds(0)))
                .filter((FilterFunction<InputData>) value -> value.getRunMode().equals(mode))
                .keyBy(e -> e.getEventSize())
                .timeWindow(Time.seconds(windowIntervalInSeconds))
                .process(new ProcessWindow());

        statsDataStream.print();

        // find the percentile of request rate
        statsDataStream.keyBy(windowStats -> windowStats.eventSize)
                .timeWindowAll(Time.seconds(entireRunWindowTimeInSeconds))
                .process(new ProcessAllWindowFunction<WindowStats, Latency, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<WindowStats> elements, Collector<Latency> out) throws Exception {
                        List<Long> latencies = new ArrayList<>();
                        for (WindowStats windowStats : elements) {
                            latencies.add(windowStats.count);
                        };
                        Latency latency = Latency.getLatency(latencies);
                        out.collect(latency);
                    }
                })
                .print();

        try {
            env.execute("calcStatsForRequestRate");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // find the request rate stats using start time as event time
    public void calcStatsForResponseRate(String csvFile, long windowIntervalInSeconds, String mode, long entireRunWindowTimeInSeconds) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<InputData> source = env.addSource(new InputDataSource(csvFile));

        DataStream<WindowStats> statsDataStream =
                source.assignTimestampsAndWatermarks(new ExtractEndTimeBoundedOutOfOrderness(Time.seconds(0)))
                .filter((FilterFunction<InputData>) value -> value.getRunMode().equals(mode))
                .keyBy(e -> e.getEventSize())
                .timeWindow(Time.seconds(windowIntervalInSeconds))
                .process(new ProcessWindow());

        //statsDataStream.print();

        // find the percentile of response rate
        statsDataStream.keyBy(windowStats -> windowStats.eventSize)
                .timeWindowAll(Time.seconds(entireRunWindowTimeInSeconds))
                .process(new ProcessAllWindowFunction<WindowStats, Latency, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<WindowStats> elements, Collector<Latency> out) throws Exception {
                        List<Long> latencies = new ArrayList<>();
                        for (WindowStats windowStats : elements) {
                            latencies.add(windowStats.count);
                        };
                        Latency latency = Latency.getLatency(latencies);
                        out.collect(latency);
                    }
                })
                .print();

        try {
            env.execute("calcStatsForRequestRate");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // find the end-to-end latency of the stream data (time taken to write and read the event)
    public void calcEndToEndLatency(String csvFile, long windowIntervalInSeconds) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<InputData> source = env.addSource(new InputDataSource(csvFile));

        DataStream<EndToEndLatency> endToEndLatencyDataStream =
                source.assignTimestampsAndWatermarks(new ExtractEndTimeBoundedOutOfOrderness(Time.seconds(windowIntervalInSeconds * 30)))
                .keyBy(e -> e.getEventKey())
                .timeWindow(Time.seconds(windowIntervalInSeconds))
                .process(new ProcessWindowByEventKey());


        endToEndLatencyDataStream.print();

        endToEndLatencyDataStream
                .keyBy(e -> e.eventSize)
                .timeWindowAll(Time.seconds(windowIntervalInSeconds))
                .process(new ProcessE2ELatencyWindow())
                .print();

        /*
        endToEndLatencyDataStream.keyBy(e -> e.eventSize)
                .flatMap(new FlatMapFunction<EndToEndLatency, Long>() {
                    @Override
                    public void flatMap(EndToEndLatency value, Collector<Long> out) throws Exception {
                        out.collect(value.latency);
                    }
                }).print();
        */


        try {
            env.execute("calcEndToEndLatency");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class ProcessE2ELatencyWindow extends ProcessAllWindowFunction<EndToEndLatency, Latency, TimeWindow> {

        @Override
        public void process(Context context, Iterable<EndToEndLatency> elements, Collector<Latency> out) throws Exception {
                List<Long> latencies = new ArrayList<>();
                for (EndToEndLatency endToEndLatency: elements) {
                    latencies.add(endToEndLatency.latency);
                };
            Latency latency = Latency.getLatency(latencies);
            out.collect(latency);
        }
    }

    public static class ProcessAllWindow extends ProcessAllWindowFunction<InputData, WindowStats, TimeWindow> {

        @Override
        public void process(Context context, Iterable<InputData> elements, Collector<WindowStats> out) throws Exception {
            out.collect(MeasureStats.process(context.window().toString(), elements));
        }
    }

    public static class ProcessWindow extends ProcessWindowFunction<InputData, WindowStats, Integer, TimeWindow> {

        @Override
        public void process(Integer integer, Context context, Iterable<InputData> elements, Collector<WindowStats> out) throws Exception {
            out.collect(MeasureStats.process(context.window().toString(),elements));
        }
    }

    public static class ProcessWindowByEventKey extends ProcessWindowFunction<InputData, EndToEndLatency, String, TimeWindow> {

        @Override
        public void process(String data, Context context, Iterable<InputData> elements, Collector<EndToEndLatency> out) throws Exception {
            EndToEndLatency endToEndLatency = new EndToEndLatency();
            long writeStartTime = 0;
            long readEndTime = 0;
            int count = 1;
            for (InputData inputData: elements) {
                if (count == 1) {
                    endToEndLatency.window = context.window().toString();
                    endToEndLatency.eventKey = inputData.getEventKey();
                    endToEndLatency.eventSize = inputData.getEventSize();
                    writeStartTime = inputData.getStartTime().toEpochMilli();
                    count++;
                } else if (count == 2) {
                    readEndTime = inputData.getEndTime().toEpochMilli();
                    endToEndLatency.latency = readEndTime - writeStartTime;
                }
            }
            out.collect(endToEndLatency);
        }
    }

    @Data
    @ToString
    public static class WindowStats implements Serializable {
        final String window;
        final long eventSize;
        final long count;
        final long bytes;
        final String bytesInMB;
        final Latency latency;
    }

    @Data
    @ToString
    public static class EndToEndLatency implements Serializable {
        private String window;
        private long eventSize;
        private String eventKey;
        private long latency;
    }

    @Data
    @ToString
    public static class EndToEndLatencyDetailed implements Serializable {
        private String window;
        private String appId;
        private String writeMode;
        private String readMode;
        private String writerThreadId;
        private String readerThreadId;
        private String eventKey;
        private long eventSize;
        private Instant writeStartTime;
        private Instant writeEndTime;
        private Instant readStartTime;
        private Instant readEndTime;
        private long latency;
        private String message;
    }

    @Data
    @ToString
    public static class AggregatedThroughputLatency implements Serializable {
        private int eventSize;
        private int count;
        private Instant startTime;
        private Instant endTime;
        private List<Long> latencies = new ArrayList<>();
        private long totalBytes = 0;

        public WindowStats calculate() {
            long elapsedTimeInSeconds = Duration.between(startTime, endTime).toMillis() / 1000;
            elapsedTimeInSeconds = (elapsedTimeInSeconds < 1) ? 1: elapsedTimeInSeconds;
            String window = "duration :" + elapsedTimeInSeconds + " seconds";
            double bytesMB = ((double) totalBytes / (1024*1024) / elapsedTimeInSeconds);
            String bytesInMB = String.format("%.2f %s", bytesMB, "MB/s");
            Latency latency = Latency.getLatency(latencies);
            WindowStats windowStats = new WindowStats(window, eventSize, count, totalBytes, bytesInMB, latency);
            return windowStats;
        }
    }

    @Data
    @ToString
    public static class Latency implements Serializable {

        static DecimalFormat df = new DecimalFormat("###.##");

        double latencyMin;
        double latencyMax;
        double latencyAvg;

        double latency50;
        double latency75;
        double latency90;
        double latency95;
        double latency99;

        public static Latency getLatency(List<Long> latencies) {
            Latency latency = new Latency();
            int size = latencies.size();
            if (size > 0) {
                Collections.sort(latencies);
                int index50 = (int) (size * 0.5);
                int index75 = (int) (size * 0.75);
                int index90 = (int) (size * 0.90);
                int index95 = (int) (size * 0.95);
                int index99 = (int) (size * 0.99);

                latency.latency50 = latencies.get(index50);
                latency.latency75 = latencies.get(index75);
                latency.latency90 = latencies.get(index90);
                latency.latency95 = latencies.get(index95);
                latency.latency99 = latencies.get(index99);

                latency.latencyMin = latencies.get(0);
                latency.latencyMax = latencies.get(size-1);

                double totalLatency = latencies.stream().collect(Collectors.summingDouble(Long::longValue));
                latency.latencyAvg = Double.valueOf(df.format(totalLatency/size));
            }
            return latency;
        }

    }

    public static class ExtractStartTimeBoundedOutOfOrderness extends BoundedOutOfOrdernessTimestampExtractor<InputData> {

        public ExtractStartTimeBoundedOutOfOrderness(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(InputData element) {
            return element.getStartTime().toEpochMilli();
        }
    }

    public static class ExtractEndTimeBoundedOutOfOrderness extends BoundedOutOfOrdernessTimestampExtractor<InputData> {

        public ExtractEndTimeBoundedOutOfOrderness(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(InputData element) {
            return element.getEndTime().toEpochMilli();
        }
    }

    public static class ExtractEventTimeBoundedOutOfOrderness extends BoundedOutOfOrdernessTimestampExtractor<InputData> {

        public ExtractEventTimeBoundedOutOfOrderness(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(InputData element) {
            return element.getEventTime().toEpochMilli();
        }
    }

    public static class ExtractStartTime extends AscendingTimestampExtractor<InputData> {
        @Override
        public long extractAscendingTimestamp(InputData element) {
            return element.getStartTime().toEpochMilli();
        }
    }

    public static class ExtractEndTime extends AscendingTimestampExtractor<InputData> {
        @Override
        public long extractAscendingTimestamp(InputData element) {
            return element.getEndTime().toEpochMilli();
        }
    }

    public static class ExtractEventTime extends AscendingTimestampExtractor<InputData> {
        @Override
        public long extractAscendingTimestamp(InputData element) {
            return element.getEventTime().toEpochMilli();
        }
    }


    public static class ExtractStartTimeBoundedOutOfOrdernessPravega extends BoundedOutOfOrdernessTimestampExtractor<Stats> {

        public ExtractStartTimeBoundedOutOfOrdernessPravega(Time maxOutOfOrderness) {
            super(maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Stats element) {
            return element.getStartTime().toEpochMilli();
        }
    }

}
