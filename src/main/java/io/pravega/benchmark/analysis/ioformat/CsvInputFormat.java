package io.pravega.benchmark.analysis.ioformat;

import io.pravega.benchmark.analysis.InputData;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CsvInputFormat extends RichInputFormat<InputData, CsvInputSplit> {

    private Reader inputReader;
    private Iterator<CSVRecord> recordsIterator;
    private String csvFilePath;
    public static String[] HEADERS = {"runMode", "appId", "threadId", "eventKey", "eventSize", "eventTime", "startTime", "endTime", "latencyInMilliSec"};

    public CsvInputFormat(String csvFilePath) {
        this.csvFilePath = csvFilePath;
    }

    @Override
    public void configure(Configuration parameters) {
        // do nothing
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public CsvInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        List<CsvInputSplit> splits = new ArrayList<>();
        int size;
        try ( Reader in = new FileReader(csvFilePath) ) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                    .withHeader(HEADERS)
                    .withFirstRecordAsHeader()
                    .parse(in);
            size = ((CSVParser) records).getRecords().size();
        }

        CsvInputSplit csvInputSplit = new CsvInputSplit(size);
        splits.add(csvInputSplit);
        return splits.toArray(new CsvInputSplit[splits.size()]);
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(CsvInputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(CsvInputSplit split) throws IOException {
        inputReader = new FileReader(csvFilePath);
        Iterable<CSVRecord> iterable = CSVFormat.DEFAULT
                .withHeader(HEADERS)
                .withFirstRecordAsHeader()
                .parse(inputReader);
        recordsIterator = iterable.iterator();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return recordsIterator.hasNext() == false;
    }

    @Override
    public InputData nextRecord(InputData reuse) throws IOException {
        CSVRecord record = recordsIterator.next();
        return parseInputData(record);
    }

    @Override
    public void close() throws IOException {
        if (inputReader != null) {
            inputReader.close();
        }
    }

    public static InputData parseInputData(CSVRecord record) {
        String runMode = record.get("runMode");
        String appId = record.get("appId");
        String threadId = record.get("threadId");
        String eventKey = record.get("eventKey");
        int eventSize = Integer.parseInt(record.get("eventSize"));
        Instant eventTime = Instant.parse(record.get("eventTime"));
        Instant startTime = Instant.parse(record.get("startTime"));
        Instant endTime = Instant.parse(record.get("endTime"));
        long latency = Long.parseLong(record.get("latencyInMilliSec"));
        InputData inputData = InputData.builder()
                .runMode(runMode)
                .appId(appId)
                .threadId(threadId)
                .eventKey(eventKey)
                .eventSize(eventSize)
                .eventTime(eventTime)
                .startTime(startTime)
                .endTime(endTime)
                .latency(latency)
                .build();
        return inputData;
    }
}
