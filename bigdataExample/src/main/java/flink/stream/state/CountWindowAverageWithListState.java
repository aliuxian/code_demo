package flink.stream.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * ListState  作为一个List是可以放很多数据的
 */
public class CountWindowAverageWithListState extends
        RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    private ListState<Tuple2<Long, Long>> eleByKey;


    @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<Tuple2<Long, Long>> descriptor = new ListStateDescriptor<>(
                    "average",
                    Types.TUPLE(Types.LONG, Types.LONG)
            );
            eleByKey = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Tuple2<Long, Double>> collector) throws Exception {

        Iterable<Tuple2<Long, Long>> value = eleByKey.get();

        if (value == null) {
            eleByKey.addAll(Collections.emptyList());
        }

        eleByKey.add(longLongTuple2);

        List<Tuple2<Long, Long>> es = Lists.newArrayList(eleByKey.get());

        if (es.size() == 3) {
            int count = 0;
            int sum = 0;
            for (Tuple2<Long, Long> e : es) {
                count++;
                sum += e.f1;
            }

            double avg = (double)sum / count;

            collector.collect(Tuple2.of(longLongTuple2.f0, avg));

            eleByKey.clear();
        }
    }

}
