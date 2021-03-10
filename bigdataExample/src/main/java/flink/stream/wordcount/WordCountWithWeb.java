package flink.stream.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCountWithWeb {

    public static void main(String[] args) throws Exception {
        // 程序入口,方便本地调试
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        // 数据获取
        DataStreamSource<String> dataStream = env.socketTextStream("local", 9999);

        // 并行度的设置,默认是cpu核数
        // 也可以在每一个operator的后面设置
        env.setParallelism(2);

        // 数据处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] fields = s.split(",");
                for (String word : fields) {
                    // 两种方式：
                    collector.collect(Tuple2.of(word, 1));
                    // collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(tuple -> tuple.f0).sum(1);
        // .keyBy(0) // 1.11开始不推荐使用这种方式了

        // 输出
        result.print();

        // 启动程序
        env.execute("WordCount");
    }
}
