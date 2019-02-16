package io.pravega.benchmark.analysis;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.time.Instant;

@Builder
@Data
@ToString
public class InputData {
    private String runMode;
    private int eventSize;
    private Instant eventTime;
    private Instant startTime;
    private Instant endTime;
    private long latency;
}
