package flink.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class WordCountWithKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "";
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", "192.168.100.100:9092");
        consumerProperties.setProperty("group.id", "test_consumer");

        FlinkKafkaConsumer flinkKafkaConsumer = new FlinkKafkaConsumer<>(topic,
                new SimpleStringSchema(),
                consumerProperties);

        // topic => partition => parallelize
        DataStreamSource dataStreamSource = env.addSource(flinkKafkaConsumer);


        SingleOutputStreamOperator result = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] fields = s.split(",");
                for (String word : fields) {
                    // 两种方式：
                    collector.collect(Tuple2.of(word, 1));
                    // collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(0).sum(1);


        result.print();

        env.execute();



    }
}
