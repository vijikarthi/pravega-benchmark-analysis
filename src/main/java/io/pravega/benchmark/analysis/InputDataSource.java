package io.pravega.benchmark.analysis;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.FileReader;
import java.io.Reader;
import java.time.Instant;

@Slf4j
public class InputDataSource implements SourceFunction<InputData> {

    public static String[] HEADERS = {"runMode", "eventSize", "eventTime", "startTime", "endTime", "latencyInMilliSec"};
    private String csvFilePath;

    public InputDataSource(String csvFilePath) {
        this.csvFilePath = csvFilePath;
    }

    @Override
    public void run(SourceContext<InputData> ctx) throws Exception {
        try ( Reader in = new FileReader(csvFilePath) ) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                    .withHeader(HEADERS)
                    .withFirstRecordAsHeader()
                    .parse(in);
            for (CSVRecord record : records) {
                String runMode = record.get("runMode");
                int eventSize = Integer.parseInt(record.get("eventSize"));
                Instant eventTime = Instant.parse(record.get("eventTime"));
                Instant startTime = Instant.parse(record.get("startTime"));
                Instant endTime = Instant.parse(record.get("endTime"));
                long latency = Long.parseLong(record.get("latencyInMilliSec"));
                InputData inputData = InputData.builder()
                        .runMode(runMode)
                        .eventSize(eventSize)
                        .eventTime(eventTime)
                        .startTime(startTime)
                        .endTime(endTime)
                        .latency(latency)
                        .build();
                ctx.collect(inputData);
            }
        }
    }

    @Override
    public void cancel() {

    }
}
