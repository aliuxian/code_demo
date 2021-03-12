package flink.stream.window;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * 使用Evictor 实现类似CountWindow(3,2)的效果
 * 每隔两个单词计算最近的三个单词
 */
public class CountWindowWordCountByEvictor {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split(",");
                for (String s1 : split) {
                    collector.collect(Tuple2.of(s1, 1));
                }
            }
        });

        // 是针对key而言的，同一个key，隔两个就计算最近三个，同时输入很多个key就是自己管自己的，别的key不影响自己的计算逻辑
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> keyedWindow = mapStream.keyBy(tuple -> tuple.f0)
                .window(GlobalWindows.create())
                .trigger(new myCountTrigger(2))
                .evictor(new myCountEvictor(3));

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = keyedWindow.sum(1);

        wordCount.print();

        env.execute(CountWindowWordCountByEvictor.class.getSimpleName());


    }

    public static class myCountTrigger extends Trigger<Tuple2<String, Integer>, GlobalWindow> {
        private long maxCount;

        public myCountTrigger(long maxCount) {
            this.maxCount = maxCount;
        }

        // 存储key对应的count值
        private ReducingStateDescriptor<Long> stateDescriptor = new ReducingStateDescriptor<Long>("count", new ReduceFunction<Long>() {
            @Override
            public Long reduce(Long aLong, Long t1) throws Exception {
                return aLong + t1;
            }
        }, Long.class);

        /**
         *  当一个元素进入到一个 window 中的时候就会调用这个方法
         * @param element   元素
         * @param timestamp 进来的时间
         * @param window    元素所属的窗口
         * @param ctx 上下文
         * @return TriggerResult
         *      1. TriggerResult.CONTINUE ：表示对 window 不做任何处理
         *      2. TriggerResult.FIRE ：表示触发 window 的计算
         *      3. TriggerResult.PURGE ：表示清除 window 中的所有数据
         *      4. TriggerResult.FIRE_AND_PURGE ：表示先触发 window 计算，然后删除 window 中的数据
         * @throws Exception
         */
        @Override
        public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
            // 拿到当前key对应的count值
            ReducingState<Long> count = ctx.getPartitionedState(stateDescriptor);
            count.add(1L);
            if (count.get() == maxCount) {
                count.clear();
                // 触发窗口计算
                return TriggerResult.FIRE;
            }
            // 否则不对窗口做任何处理
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
            // 如果有基于process time的定时器任务，可以在这实现
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
            // 如果有基于event time的定时器任务，可以在这实现
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow globalWindow, TriggerContext ctx) throws Exception {
            // 清除状态值
            ctx.getPartitionedState(stateDescriptor).clear();
        }
    }

    public static class myCountEvictor implements Evictor<Tuple2<String, Integer>, GlobalWindow> {
        // window大小
        private long windowSize;

        public myCountEvictor(long windowSize) {
            this.windowSize = windowSize;
        }


        /**
         *  在 window 计算之前删除特定的数据
         * @param elements  window 中所有的元素
         * @param size  window 中所有元素的大小
         * @param window    window
         * @param evictorContext    上下文
         */
        @Override
        public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
            if (size <= windowSize) {
                return;
            } else {
                int evictorCount = 0;
                Iterator<TimestampedValue<Tuple2<String, Integer>>> iterator = elements.iterator();
                while (iterator.hasNext()) {
                    iterator.next();
                    evictorCount++;
                    if (size - windowSize >= evictorCount) {
                        iterator.remove();
                    } else {
                        break;
                    }
                }
            }
        }

        /**
         *  在 window 计算之后删除特定的数据
         * @param elements  window 中所有的元素
         * @param size  window 中所有元素的大小
         * @param window    window
         * @param evictorContext    上下文
         */
        @Override
        public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {

        }
    }
    
    
}
