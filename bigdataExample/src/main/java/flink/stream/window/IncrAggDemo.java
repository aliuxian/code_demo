package flink.stream.window;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 增量聚合
 * 窗口内累加，来一条累加一次，时间到再输出
 */
public class IncrAggDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 老的API需要指定
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> intStream = dataStream.map(x -> Integer.valueOf(x));

        AllWindowedStream<Integer, TimeWindow> windowedStream = intStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // 使用这种API，需要指定使用什么时间
        // env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        AllWindowedStream<Integer, TimeWindow> windowedStream = intStream.timeWindowAll(Time.seconds(10));


        windowedStream.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer integer, Integer t1) throws Exception {
                System.out.println(integer +  " + " + t1);
                return integer + t1;
            }
        }).print();

        env.execute(IncrAggDemo.class.getSimpleName());

    }
}
