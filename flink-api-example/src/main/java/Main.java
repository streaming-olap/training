import com.xiaoxiang.flink.ConnectSource;
import com.xiaoxiang.flink.Record;
import com.xiaoxiang.flink.SocketSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by yuhailin.
 */
public class Main {


    private static boolean checkpoint = false;

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        if (checkpoint) {
            // 设置checkpoint
            env.enableCheckpointing(60000);
            CheckpointConfig checkpointConf = env.getCheckpointConfig();
            checkpointConf.setMinPauseBetweenCheckpoints(30000L);
            checkpointConf.setCheckpointTimeout(10000L);
            checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        } else {
            // non checkpoint
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                    Integer.MAX_VALUE, // number of restart attempts
                    Time.of(5, TimeUnit.SECONDS) // delay
            ));

        }


        //为什么要 broadcast ？
        //配置流
        DataStream<String> configDataStream = env.addSource(new SocketSource())
                                                 .broadcast();

        DataStream<Record>  dataStream = env.addSource(new ConnectSource());


        ConnectedStreams<Record, String>  connectedStreams = dataStream.connect(configDataStream);

        DataStream<Record> flatMapDataStream =  connectedStreams.flatMap(new CoFlatMapFunction<Record, String, Record>() {

            private String config;

            @Override
            public void flatMap1(Record record, Collector<Record> collector) throws Exception {
                /*
                 *
                 *
                 * 处理业务逻辑
                 *
                 * */

                if (config.equals("0")) {
                    collector.collect(record);
                } else if(config.equals("1")) {
                    collector.collect(record);
                }
            }

            @Override
            public void flatMap2(String s, Collector<Record> collector) throws Exception {
                /*
                 *   处理配置
                 */

                config = s;
            }
        });


        SplitStream<Record> splitStream = dataStream.split(new OutputSelector<Record>() {

            @Override
            public Iterable<String> select(Record record) {

                List<String> output = new ArrayList<String>();

                String biz = "" + record.getBizId();
                output.add(biz);

                return output;

            }
        });


        splitStream.select("1").addSink(new SinkFunction<Record>() {
            @Override
            public void invoke(Record record) throws Exception {

            }
        });

        splitStream.select("2").addSink(new SinkFunction<Record>() {
            @Override
            public void invoke(Record record) throws Exception {

            }
        });
    }
}
