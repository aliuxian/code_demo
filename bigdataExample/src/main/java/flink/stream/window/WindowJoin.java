package flink.stream.window;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream1 = env.socketTextStream("localhost", 9998);
        DataStreamSource<String> dataStream2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, String>> mapStream1 = dataStream1.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                System.out.println("stream1 " + s);
                String[] split = s.split(",");
                return new Tuple2<>(split[0], split[1]);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, String>> mapStream2 = dataStream2.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                System.out.println("stream2 " + s);
                String[] split = s.split(",");
                return new Tuple2<>(split[0], split[1]);
            }
        });

        mapStream1.join(mapStream2)
                .where(tuple -> tuple.f0)
                .equalTo(tuple -> tuple.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(20)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple2<String, String>, String>() {
                    @Override
                    public String join(Tuple2<String, String> data1, Tuple2<String, String> data2) throws Exception {
                        System.out.println("data1: " + data1.toString() + "  data2: " + data2);
                        return data1.f0 + "," + data1.f1 + "," + data2.f1;
                    }
                }).print();


        env.execute(WindowJoin.class.getSimpleName());


    }
}
