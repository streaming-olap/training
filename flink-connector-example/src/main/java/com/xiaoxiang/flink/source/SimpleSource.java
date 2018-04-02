package com.xiaoxiang.flink.source;

import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SimpleSource implements SourceFunction<Long>, Checkpointed<Long> {

    private long count = 0L;
    private volatile boolean isRunning = true;

    public void run(SourceContext<Long> ctx) {
        while (isRunning && count < 1000) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(count);
                count++;
            }
        }
    }

    public void cancel() {
        isRunning = false;
    }

    public Long snapshotState(long checkpointId, long checkpointTimestamp) { return count; }

    public void restoreState(Long state) { this.count = state; }


}
