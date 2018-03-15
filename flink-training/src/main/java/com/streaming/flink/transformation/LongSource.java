package com.streaming.flink.transformation;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

/**
 * Created by Jying on 2018/3/14.
 */
public class LongSource extends RichParallelSourceFunction<Integer> {

    private volatile boolean isRunning = true;
    private Random rand = new Random(47);


    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        while (isRunning){
            for(int i = 0; i < 10; i++) {
                sourceContext.collect(i);
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
