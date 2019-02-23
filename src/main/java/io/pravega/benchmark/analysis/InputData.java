package io.pravega.benchmark.analysis;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.time.Instant;

@Builder
@AllArgsConstructor
@Data
@ToString
public class InputData implements Serializable {
    public InputData() {}
    private String runMode;
    private String appId;
    private String threadId;
    private String eventKey;
    private int eventSize;
    private Instant eventTime;
    private Instant startTime;
    private Instant endTime;
    private long latency;
}
