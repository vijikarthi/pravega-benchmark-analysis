package io.pravega.benchmark.analysis;

import io.pravega.benchmark.loadtest.reports.Stats;
import io.pravega.client.stream.Stream;
import io.pravega.connectors.flink.FlinkPravegaReader;

public class AppMain {

    public static void main(String ... args) {

        String dir = "/data1/workspace/vijikarthi-github/pravega-benchmark/build/install/pravega-benchmark";
        String csvFile = dir + "/" + "perfTest-write-changeme.csv";

        long windowInterval = 1;
        long slideInterval = 5;
        long totalRunIntervalInSeconds = 10000;

        MeasureStats stats = new MeasureStats();

        //stats.getThroughputAndLatencyForTheEntireRunStreaming(csvFile, totalRunIntervalInSeconds, "write");
        //stats.getThroughputAndLatencyForTheEntireRunStreaming(csvFile, totalRunIntervalInSeconds, "read");

        //stats.calcStatsForRequestRate(csvFile, 1, "write", totalRunIntervalInSeconds);
        //stats.calcStatsForRequestRate(csvFile, 1, "read", totalRunIntervalInSeconds);

        //stats.calcEndToEndLatency(csvFile, 60000);

        //......
        //stats.getThroughputAndLatencyForTheEntireRunBatch(csvFile, "write");
        //stats.getThroughputAndLatencyForTheEntireRunBatch(csvFile, "read");

        //stats.calcStatsForFixedWindowInterval(csvFile, windowInterval, "write");
        //stats.calcStatsForFixedWindowInterval(csvFile, windowInterval, "read");

        //stats.calcStatsForResponseRate(csvFile, 1, "write", totalRunIntervalInSeconds);
        //stats.calcStatsForResponseRate(csvFile, 1, "read", totalRunIntervalInSeconds);

        //stats.calcStatsForFixedWindowIntervalNonKeyed(csvFile, windowInterval, "write");
        //stats.calcStatsForFixedWindowIntervalNonKeyed(csvFile, windowInterval, "read");

        //stats.calcStatsForSlidingWindowInterval(csvFile, windowInterval, slideInterval, "write");
        //stats.calcStatsForSlidingWindowInterval(csvFile, windowInterval, slideInterval, "read");


        /*
        Stream stream = Stream.of("report","test1");
        String controllerUri = "tcp://localhost:9090";
        FlinkPravegaReader<Stats> flinkPravegaReader = MeasureStats.getFlinkPravegaReader(stream, controllerUri);
        stats.getThroughputAndLatencyForTheEntireRunStreamingFromPravega(flinkPravegaReader, windowInterval, "write");
        */

    }
}
