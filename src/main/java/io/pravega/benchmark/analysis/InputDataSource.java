package io.pravega.benchmark.analysis;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.FileReader;
import java.io.Reader;

import static io.pravega.benchmark.analysis.ioformat.CsvInputFormat.HEADERS;
import static io.pravega.benchmark.analysis.ioformat.CsvInputFormat.parseInputData;

@Slf4j
public class InputDataSource implements SourceFunction<InputData> {

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
                ctx.collect(parseInputData(record));
            }
        }
    }

    @Override
    public void cancel() {

    }
}
