package io.pravega.benchmark.analysis;

import io.pravega.benchmark.analysis.ioformat.CsvInputFormat;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
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

import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class MeasureStats {

    public static Stats process(String window, Iterable<InputData> elements) {
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
        Stats stats = new Stats(window, eventSize, count, bytes, bytesInMB, latency);
        return stats;
    }

    // find the throughput and latency for the entire run
    public void getThroughputAndLatencyForTheEntireRun(String csvFile) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        try {
            CsvInputFormat inputFormat = new CsvInputFormat(csvFile);
            DataSet<InputData> dataSet = env.createInput(inputFormat);
            List<InputData> inputDataList = dataSet.collect();
            int size = inputDataList.size();
            if (size <= 0) {
                log.info("No results found from the csv file");
                return;
            }

            Instant startTime = inputDataList.get(0).getStartTime();
            Instant endTime = inputDataList.get(size-1).getEndTime();
            long elapsedTimeInSeconds = Duration.between(startTime, endTime).toMillis() / 1000;

            List<Long> latencies = new ArrayList<>();
            long totalBytes = 0;
            int count = 0;
            for (InputData inputData: inputDataList) {
                latencies.add(inputData.getLatency());
                totalBytes += inputData.getEventSize();
                count++;
            }

            long eventSize = inputDataList.get(0).getEventSize();
            String window = "duration :" + elapsedTimeInSeconds + " seconds";
            double bytesMB = ((double) totalBytes / (1024*1024) / elapsedTimeInSeconds);
            String bytesInMB = String.format("%.2f %s", bytesMB, "MB/s");
            Latency latency = Latency.getLatency(latencies);
            Stats stats = new Stats(window, eventSize, count, totalBytes, bytesInMB, latency);
            log.info("{}", stats);

        } catch (Exception e) {
            log.error("unable to calculate the stats", e);
        }
    }

    // find the response throughput and latency for a window interval using non-keyed stream
    public void calcStatsForFixedWindowIntervalNonKeyed(String csvFile, long windowIntervalInSeconds) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<InputData> source = env.addSource(new InputDataSource(csvFile));

        source.assignTimestampsAndWatermarks(new ExtractEndTimeBoundedOutOfOrderness(Time.seconds(windowIntervalInSeconds * 5)))
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
    public void calcStatsForFixedWindowInterval(String csvFile, long windowIntervalInSeconds) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<InputData> source = env.addSource(new InputDataSource(csvFile));

        source.assignTimestampsAndWatermarks(new ExtractEndTimeBoundedOutOfOrderness(Time.seconds(windowIntervalInSeconds * 5)))
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
    public void calcStatsForSlidingWindowInterval(String csvFile, long windowIntervalInSeconds, long windowSlideIntervalInSeconds) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<InputData> source = env.addSource(new InputDataSource(csvFile));

        source.assignTimestampsAndWatermarks(new ExtractEndTimeBoundedOutOfOrderness(Time.seconds(windowIntervalInSeconds * 5)))
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
    public void calcStatsForRequestRate(String csvFile, long windowIntervalInSeconds) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        DataStream<InputData> source = env.addSource(new InputDataSource(csvFile));

        source.assignTimestampsAndWatermarks(new ExtractStartTimeBoundedOutOfOrderness(Time.seconds(windowIntervalInSeconds * 5)))
                .keyBy(e -> e.getEventSize())
                .timeWindow(Time.seconds(windowIntervalInSeconds))
                .process(new ProcessWindow())
                .print();

        try {
            env.execute("calcStatsForRequestRate");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class ProcessAllWindow extends ProcessAllWindowFunction<InputData, Stats, TimeWindow> {

        @Override
        public void process(Context context, Iterable<InputData> elements, Collector<Stats> out) throws Exception {
            out.collect(MeasureStats.process(context.window().toString(), elements));
        }
    }

    public static class ProcessWindow extends ProcessWindowFunction<InputData, Stats, Integer, TimeWindow> {

        @Override
        public void process(Integer integer, Context context, Iterable<InputData> elements, Collector<Stats> out) throws Exception {
            out.collect(MeasureStats.process(context.window().toString(),elements));
        }
    }

    @Data
    @ToString
    public static class Stats {
        final String window;
        final long eventSize;
        final long count;
        final long bytes;
        final String bytesInMB;
        final Latency latency;
    }

    @Data
    @ToString
    public static class Latency {

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

}
