package com.xiaoxiang.flink;

import com.xiaoxiang.flink.source.SimpleSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Launcher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //set the checkpoint interval
       streamExecutionEnvironment.addSource(new SimpleSource());

        streamExecutionEnvironment.execute("flink-connector-example");
    }
}
