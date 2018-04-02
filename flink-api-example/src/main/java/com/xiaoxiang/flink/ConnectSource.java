package com.xiaoxiang.flink;

import org.apache.flink.types.Row;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.sql.Timestamp;
import java.util.Random;

/**
 * Created by yuhailin on 2017/10/16.
 */
public class ConnectSource implements ParallelSourceFunction<Record> {

    private static Random random = new Random();

    /**
     *
     *  产生数据
     */
    @Override
    public void run(SourceContext<Record> sourceContext) throws Exception {
        while (true) {

            /*
             *    v1 业务线
             *    v2 业务Id
             *    v3 业务属性值
             *    v4 时间戳
             *    ....
             */

            Random random = new Random(100);

            Record record = new Record();


            for (int i = 0 ; i < 4 ; i++) {
                record.setBizName("" + i);
                record.setBizId(i);
                record.setAttr(Integer.valueOf(random.nextInt() / 10));
                record.setData("json string or other");
                record.setTimestamp(new Long(System.currentTimeMillis()) / 1000);

                sourceContext.collect(record);
            }

            Thread.sleep(200);
        }
    }

    /**
     * 关闭资源
     */
    @Override
    public void cancel() {
    }

}