package flink.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 面向对象的思想
 */
public class WordCountForObject {

    public static void main(String[] args) throws Exception {
        // 程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 数据获取
        DataStreamSource<String> dataStream = env.socketTextStream("local", 9999);

        // 数据处理
        SingleOutputStreamOperator<WordAndOne> result = dataStream
                .flatMap(new SplitFlatMap())
                .keyBy(tuple -> tuple.word)
                .sum("one");

        // 输出
        result.print();

        // 启动程序
        env.execute("WordCount");
    }

    public static class SplitFlatMap implements FlatMapFunction<String, WordAndOne> {

        @Override
        public void flatMap(String s, Collector<WordAndOne> collector) throws Exception {
            String[] words = s.split(",");
            for (String word : words) {
                collector.collect(new WordAndOne(word, 1));
            }
        }
    }



    public static class WordAndOne {
        private String word;
        private Integer one;

        public WordAndOne() {

        }

        public WordAndOne(String word, Integer one) {
            this.word = word;
            this.one = one;
        }

        @Override
        public String toString() {
            return "WordAndOne{" +
                    "word='" + word + '\'' +
                    ", one=" + one +
                    '}';
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getOne() {
            return one;
        }

        public void setOne(Integer one) {
            this.one = one;
        }
    }
}
