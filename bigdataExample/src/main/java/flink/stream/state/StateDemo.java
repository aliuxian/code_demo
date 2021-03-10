package flink.stream.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.omg.CORBA.LongLongSeqHelper;


/**
 * 根据key每三个元素求一次平均值
 */
public class StateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, Long>> dataStream = env.fromElements(
                Tuple2.of(1l, 3l),
                Tuple2.of(1l, 5l),
                Tuple2.of(2l, 5l),
                Tuple2.of(2l, 5l),
                Tuple2.of(1l, 5l),
                Tuple2.of(1l, 6l),
                Tuple2.of(2l, 4l));

        dataStream.keyBy(tuple -> tuple.f0)
                .flatMap(new CountWindowAverageWithMapState())
                .print();

        env.execute("valueStateDemo");
    }
}
