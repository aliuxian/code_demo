package flink.stream.pro_demo;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class AverageForWindowDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> intStream = dataStream.map(x -> Integer.valueOf(x));

        AllWindowedStream<Integer, TimeWindow> windowAllWindowedStream = intStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        // 编码改进：将AggregateFunction 抽离出去，面向对象编程
        windowAllWindowedStream.aggregate(new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
            @Override
            public Tuple2<Integer, Integer> createAccumulator() {
                return new Tuple2<>(0, 0);
            }

            @Override
            public Tuple2<Integer, Integer> add(Integer integer, Tuple2<Integer, Integer> acc) {
                return new Tuple2<>(acc.f0 + 1, acc.f1 + integer);
            }

            @Override
            public Double getResult(Tuple2<Integer, Integer> acc) {
                return acc.f1 * 1.0/acc.f0;
            }

            @Override
            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> acc, Tuple2<Integer, Integer> acc1) {
                return new Tuple2<>(acc.f0 + acc1.f0, acc.f1 + acc1.f1);
            }
        }).print();


        env.execute(AverageForWindowDemo.class.getSimpleName());

    }
}
