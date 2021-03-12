package flink.stream.window;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;


/**
 * 全量聚合，窗口内元素到齐之后才开始处理元素，例如在窗口内对数据进行排序
 * 可以和IncrAggDemo对比，
 * 一个是全部到齐计算好了再返回
 * 一个是来一个算一个
 */
public class FullAggDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> intStream = dataStream.map(x -> Integer.valueOf(x));

        AllWindowedStream<Integer, TimeWindow> windowedStream = intStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        windowedStream.process(new ProcessAllWindowFunction<Integer, Integer, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Integer> iterable, Collector<Integer> collector) throws Exception {
                System.out.println("执行逻辑");
                Iterator<Integer> iterator = iterable.iterator();
                int sum = 0;
                while (iterator.hasNext()) {
                    sum += iterator.next();
                }
                collector.collect(sum);
            }
        }).print();

        env.execute(FullAggDemo.class.getSimpleName());


    }
}
