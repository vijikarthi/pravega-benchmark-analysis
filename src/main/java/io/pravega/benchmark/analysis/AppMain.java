package io.pravega.benchmark.analysis;

public class AppMain {

    public static void main(String ... args) {

        //String csvFile = "/data1/workspace/vijikarthi-github/pravega-benchmark/write-results-1550358613690.csv";
        String csvFile = "/data1/workspace/vijikarthi-github/pravega-benchmark/read-results-1550358651545.csv";
        long windowInterval = 1;
        long slideWindowInterval = 5;
        long slideInterval = 2;
        MeasureStats stats = new MeasureStats();

        //stats.getThroughputAndLatencyForTheEntireRun(csvFile);
        stats.calcStatsForFixedWindowInterval(csvFile, windowInterval);


        //stats.calcStatsForRequestRate(csvFile, windowInterval);

        //stats.calcStatsForFixedWindowIntervalNonKeyed(csvFile, windowInterval);
        //stats.calcStatsForSlidingWindowInterval(csvFile, slideWindowInterval, slideInterval);

    }
}
