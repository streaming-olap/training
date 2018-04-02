import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        // the port to connect to
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("127.0.0.1", port, "\n");


        DataStream<String> text1 = text.map(new MapFunction<String, String>() {
                @Override
                public String map(String s) throws Exception {
                    if (s.equals("23")) {
                        return "123+++++";
                    }

                    return null;
                }
        })
        .setParallelism(3)
        .rescale();

        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = text1
                .flatMap(new FlatMapFunction<String, WordWithCount>() {
                    @Override
                    public void flatMap(String value, Collector<WordWithCount> out) {

                        if (value != null) {
                            for (String word : value.split("\\s")) {
                                out.collect(new WordWithCount(word, 1L));
                            }
                        }
                    }
                })
                .keyBy("word")

                .timeWindow(Time.seconds(5))

                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        return new WordWithCount(a.word, a.count + b.count);
                    }
                });

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(3);

        env.execute("Socket Window WordCount");
    }

    // Data type for words with count
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}