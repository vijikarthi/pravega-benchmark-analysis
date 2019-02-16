package io.pravega.benchmark.analysis.ioformat;

import org.apache.flink.core.io.InputSplit;

public class CsvInputSplit implements InputSplit {

    private int splitNumber;

    public CsvInputSplit(int splitNumber) {
        this.splitNumber = splitNumber;
    }

    @Override
    public int getSplitNumber() {
        return splitNumber;
    }
}
