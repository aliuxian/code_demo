package flink.stream.pro_demo;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;
import java.sql.Timestamp;


/**
 * 以小时为维度计算各个省份的广告点击量
 * 同时监控异常的点击行为，比如同一个用户对同一条广告在一个小时内点击了100次以上
 */
public class AdClickCount {
    public  static OutputTag<Tuple3<Long,Long,String>> blackListOutputTag = new OutputTag<Tuple3<Long,Long,String>>("blacklist"){};

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //指定checkpoints的信息

        SingleOutputStreamOperator<AdClickEvent> adEventStream = env.readTextFile(Utils.CLICK_LOG_PATH) //读取数据
                .map(new ParseAdClickLog()) //解析数据
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                                .withTimestampAssigner((ctx) -> new TimeStampExtractor())); //指定时间字段
        //计算黑名单
        // keyBy 的 key 不是一个单一的字段，而是几个字段组合的，所以用  KeySelector 来实现这样的需求
        // 这里以用户ID和广告ID为key
        SingleOutputStreamOperator<AdClickEvent> adClickFilterBlackStream = adEventStream.keyBy(new getKey())
                .process(new FilterBlackListUser(100)); //用来计算黑名单


        //打印黑名单（侧输出流）
        adClickFilterBlackStream.getSideOutput(blackListOutputTag)
                .print();

        //计算各个省份的广告点击量
        adClickFilterBlackStream.keyBy( adclick -> adclick.province)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new AdClickCountAgg(),new AdClickWindow())
                .print();

        env.execute("AdClickCount");

    }

    /**
     * IN, OUT, KEY, W extends Window
     */
    public static class AdClickWindow
            implements WindowFunction<Long,CountByProvince,String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow timeWindow,
                          Iterable<Long> input,
                          Collector<CountByProvince> out) throws Exception {

            out.collect(new CountByProvince(new Timestamp(timeWindow.getEnd()).toString(),
                    key,input.iterator().next()));  // input.iterator().next()  因为是按key聚合，所以一个key就只有一个结果，即不需要遍历

        }
    }


    /**
     * 实现一个sum的效果
     */
    public static class AdClickCountAgg implements AggregateFunction<AdClickEvent,Long,Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent adClickEvent, Long acc) {
            return acc + 1L;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    /**
     * 指定一个组合的Key
     */
    public static class getKey implements KeySelector<AdClickEvent, Tuple2<Long,Long>> {
        @Override
        public Tuple2<Long, Long> getKey(AdClickEvent adClickEvent) throws Exception {
            return Tuple2.of(adClickEvent.getUserId(),adClickEvent.getAdId());
        }
    }


    /**
     * 计算黑名单
     *
     * K,
     * I,
     * O
     *
     *  按小时为粒度，创建定时器，每一个小时清空一次黑名单
     *  并将黑名单从侧输出流输出
     */
    public static class FilterBlackListUser
            extends KeyedProcessFunction<Tuple2<Long,Long>,
                        AdClickEvent,AdClickEvent> {
        public int maxCount;

        //保存当前用户对当前广告的点击量
        public ValueState<Long> countState;
        //保存是否发送过黑名单
        public ValueState<Boolean> isSetBlackList;
        //保存定时器触发的时间戳
        public ValueState<Long> resetTimeer;

        public FilterBlackListUser(int maxCount) {
            this.maxCount = maxCount;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("count-state", Long.class);
            countState = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor<Boolean> descriptor1 = new ValueStateDescriptor<>("issent-state", Boolean.class);
            new ValueStateDescriptor<>("issent-state", Boolean.class);

            isSetBlackList =  getRuntimeContext().getState(descriptor1);

            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<>("resettime-state", Long.class);

            resetTimeer = getRuntimeContext().getState(descriptor2);
        }


        @Override
        public void processElement(AdClickEvent adClickEvent,
                                   Context ctx,
                                   Collector<AdClickEvent> out) throws Exception {

            Long currentCount = countState.value();

            // 这个key是第一次出现，进行相应的初始化
            if(currentCount == null){
                currentCount = 0L;
                isSetBlackList.update(false);
                //计算时间，整点时间
                long ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24);
                resetTimeer.update(ts);
                //注册定时器
                ctx.timerService().registerProcessingTimeTimer(ts);
            }

            if(currentCount >= maxCount){

                Boolean value = isSetBlackList.value();
                //如果没有发送过告警
                if(!isSetBlackList.value()){
                    //把发送消息的状态修改位true。
                    isSetBlackList.update(true);

                    //通过侧输出流，用的是Context   如果不是侧输出流，是正常的输出，那么就是Collector
                    //这个地方就看大家的需求，需求需要怎么处理，你就怎么处理就可以。
                    ctx.output(blackListOutputTag,
                            Tuple3.of(adClickEvent.userId,adClickEvent.adId,"点击超过" + maxCount +" 次"));
                }
                return;
            }
            //累加点击的次数
            countState.update(currentCount + 1);
            out.collect(adClickEvent);
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            if(timestamp == resetTimeer.value()){
                isSetBlackList.clear();
                countState.clear();
                resetTimeer.clear();
            }
        }



    }


    /**
     * watermark计算
     */
    private static class PeriodicWatermarkGenerator implements WatermarkGenerator<AdClickEvent>, Serializable {

        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        private long currentMaxEventTime = 0L;
        private long maxOutOfOrderness = 10L; // 最大允许的乱序时间 10 秒

        // 当前接收到的事件的最大时间，每一个事件都会调用这个方法
        @Override
        public void onEvent(
                AdClickEvent event, long eventTimestamp, WatermarkOutput output) {
            long currentElementEventTime = event.timestamp;
            currentMaxEventTime = Math.max(currentMaxEventTime, currentElementEventTime);
        }

        // 计算watermark：当前最大的事件时间减去允许延迟的时间
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

            output.emitWatermark(new Watermark((currentMaxEventTime - maxOutOfOrderness) * 1000 ));
        }
    }


    /**
     * 指定时间字段（使用事件时间的时候需要指定事件时间在日志中的位置）
     */
    private static class TimeStampExtractor implements TimestampAssigner<AdClickEvent> {
        @Override
        public long extractTimestamp(AdClickEvent element, long recordTimestamp) {
            return element.timestamp * 1000;
        }
    }



    /**
     * 解析数据
     */
    public static class ParseAdClickLog implements MapFunction<String,AdClickEvent> {
        @Override
        public AdClickEvent map(String line) throws Exception {
            String[] dataArray = line.split(",");
            return new AdClickEvent(Long.parseLong(dataArray[0].trim()),
                    Long.parseLong(dataArray[1].trim()),
                    dataArray[2].trim(),
                    dataArray[3].trim(),
                    Long.parseLong(dataArray[4].trim()));
        }
    }



    /**
     * 用户点击事件的类
     */
    public static class AdClickEvent{
        private Long userId; //用户ID
        private Long adId; //广告ID
        private String province; //省份
        private String city; //城市
        private Long timestamp; //点击广告的事件时间

        public AdClickEvent(){

        }

        @Override
        public String toString() {
            return "AdClickEvent{" +
                    "userId=" + userId +
                    ", adId=" + adId +
                    ", province='" + province + '\'' +
                    ", city='" + city + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }

        public AdClickEvent(Long userId, Long adId, String province,
                            String city, Long timestamp) {
            this.userId = userId;
            this.adId = adId;
            this.province = province;
            this.city = city;
            this.timestamp = timestamp;
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public Long getAdId() {
            return adId;
        }

        public void setAdId(Long adId) {
            this.adId = adId;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }
    }




    /**
     * 结果类
     */
    public static class CountByProvince {
        private String windowEnd;
        private String province;
        private Long count;

        public CountByProvince() {

        }

        @Override
        public String toString() {
            return "CountByProvince{" +
                    "windowEnd='" + windowEnd + '\'' +
                    ", province='" + province + '\'' +
                    ", count=" + count +
                    '}';
        }

        public CountByProvince(String windowEnd, String province, Long count) {
            this.windowEnd = windowEnd;
            this.province = province;
            this.count = count;
        }

        public String getWindowEnd() {
            return windowEnd;
        }

        public void setWindowEnd(String windowEnd) {
            this.windowEnd = windowEnd;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }
    }
}
