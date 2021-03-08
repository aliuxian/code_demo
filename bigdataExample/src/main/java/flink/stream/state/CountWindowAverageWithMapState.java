package flink.stream.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * MapState  相同的key存在覆盖的情况
 * 不太适合求平均值这个场景，仅仅用于举例
 */
public class CountWindowAverageWithMapState extends
        RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    private MapState<String, Long> mapState;


    @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>(
                "average",
                String.class,
                Long.class);
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Tuple2<Long, Double>> collector) throws Exception {

        // key会出现覆盖，所以换成uuid
        mapState.put(UUID.randomUUID().toString(), longLongTuple2.f1);

        List<Long> values = Lists.newArrayList(mapState.values());

        if (values.size() == 3) {
            int count = 0;
            int sum = 0;
            for (long value : values) {
                count++;
                sum += value;
            }

            double avg = (double)sum / count;

            collector.collect(Tuple2.of(longLongTuple2.f0, avg));

            mapState.clear();
        }

    }

}
