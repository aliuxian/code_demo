package flink.stream.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


/**
 * ValueState只可以放一条数据
 */
public class CountWindowAverageWithValueState extends
        RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    // key表示输入的key出现的次数
    // value表示出现的key的value的和
    private ValueState<Tuple2<Long, Long>> countAndSum;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor<Tuple2<Long, Long>> valueStateDescriptor = new ValueStateDescriptor<>(
                "average",
                Types.TUPLE(Types.LONG, Types.LONG)
        );
        countAndSum = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Tuple2<Long, Double>> collector) throws Exception {
        Tuple2<Long, Long> value = countAndSum.value();

        if (value == null) {
            value = Tuple2.of(0L, 0L);
        }

        value.f0 += 1;
        value.f1 += longLongTuple2.f1;

        countAndSum.update(value);

        if (value.f0 == 3) {
            double ave = (double)value.f1 / value.f0;
            collector.collect(Tuple2.of(longLongTuple2.f0, ave));
            countAndSum.clear();
        }
    }




}
