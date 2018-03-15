package com.streaming.flink;

import com.streaming.flink.transformation.LongSource;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created by Jying on 2018/3/14.
 */
public class ChooseEvenNumber {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment().setParallelism(1);

        env.addSource(new LongSource())
        .setParallelism(2)
        .filter(new RichFilterFunction<Integer>() {
            private Counter filterOutNumber;
            @Override
            public void open(Configuration parameters) throws Exception {
                 filterOutNumber = getRuntimeContext()
                        .getMetricGroup()
                        .addGroup("minwenjun")
                        .counter("filterOutNumber");
            }

            @Override
            public boolean filter(Integer integer) throws Exception {
                if (integer % 2 == 0) {
                    filterOutNumber.inc();
                }
                return !(integer % 2 == 0);
            }
        }).print();

        env.execute();
    }
}
