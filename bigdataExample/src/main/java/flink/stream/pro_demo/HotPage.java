package flink.stream.pro_demo;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HotPage {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.readTextFile(Utils.PAGE_LOG_PAH)
                .map(new ParseLog())
                .assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .forGenerator((ctx) ->  new PeriodicWatermarkGenerator())
                        .withTimestampAssigner((ctx) -> new TimeStampExtractor())
                )
                .keyBy(apacheLog -> apacheLog.url)
                .window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .aggregate(new PageCountAgg(), new PageWindowResult())
                .keyBy(urlView -> urlView.windowEnd)
                .process(new TopNHotPage(3))
                .print();

                env.execute("HotPage");
    }

    /**
     * ??????????????????
     * K, I, O
     */
    public static class TopNHotPage extends KeyedProcessFunction<Long, UrlView, String> {

        private int topN = 0;

        public TopNHotPage(int topN) {
            this.topN = topN;
        }

        public int getTopN() {
            return topN;
        }

        public void setTopN(Integer topN) {
            this.topN = topN;
        }

        // url  ??????
        public MapState<String, Long> urlState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>(
                    "url_count",
                    String.class,
                    Long.class
            );
            urlState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(UrlView urlView, Context context, Collector<String> collector) throws Exception {
            urlState.put(urlView.getUrl(), urlView.getCount());

            context.timerService().registerEventTimeTimer(urlView.windowEnd + 1);
        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<UrlView> urlView = new ArrayList<>();
            ArrayList<String> allElementKey = Lists.newArrayList(urlState.keys());

            for (String url : allElementKey) {
                urlView.add(new UrlView(url, new Timestamp(timestamp - 1).getTime(), urlState.get(url).longValue()));
            }

            Collections.sort(urlView);

            List<UrlView> topN;
            if (urlView.size() >= this.topN) {
                topN = urlView.subList(0, this.topN);
            } else {
                topN = urlView;
            }


            for (UrlView view : topN) {
                out.collect(view.toString());
            }
        }
    }

    /**
     * IN, ?????????????????????
     * OUT, ?????????????????????
     * KEY, key
     * W <: Window window?????????
     *
     */
    public static class PageWindowResult implements WindowFunction<Long, UrlView, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow timeWindow, Iterable<Long> iterable, Collector<UrlView> collector) throws Exception {
            collector.collect(
                    new UrlView(s, timeWindow.getEnd(), iterable.iterator().next())
            );
        }
    }

    /**
     * ApacheLogEvent, ??????
     * Long, ???????????????????????????
     * Long ?????????URL???????????????
     * ???????????????sum?????????
     */
    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
            return aLong + 1L;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    /**
     * ????????????????????????????????????ApacheLogEvent??????
     */
    public static class ParseLog implements MapFunction<String, ApacheLogEvent> {
        @Override
        public ApacheLogEvent map(String s) throws Exception {
            String[] fields = s.split(" ");
            SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            long timeStamp = dateFormat.parse(fields[3].trim()).getTime();
            return new ApacheLogEvent(fields[0].trim(),fields[1].trim(),timeStamp,
                    fields[5].trim(),fields[6].trim());
        }
    }


    /**
     * ??????watermark
     */
    private static class PeriodicWatermarkGenerator implements WatermarkGenerator<ApacheLogEvent>, Serializable {

        private long currentMaxEventTime = 0L;
        private long maxOutOfOrderness = 10L; // ??????????????????????????? 10 ???

        @Override
        public void onEvent(
                ApacheLogEvent event, long eventTimestamp, WatermarkOutput output) {
            long currentElementEventTime = event.eventTime;
            currentMaxEventTime = Math.max(currentMaxEventTime, currentElementEventTime);

        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

            output.emitWatermark(new Watermark((currentMaxEventTime - maxOutOfOrderness) * 1000));
        }
    }

    /**
     * ????????????????????????
     */
    private static class TimeStampExtractor implements TimestampAssigner<ApacheLogEvent> {
        @Override
        public long extractTimestamp(ApacheLogEvent element, long recordTimestamp) {
            return element.eventTime;
        }
    }

    /**
     * ????????????
     */
    public static class ApacheLogEvent {
        private String ip;
        private String userId;
        private Long eventTime;
        private String method;
        private String url;

        public ApacheLogEvent(){

        }

        public ApacheLogEvent(String ip, String userId, Long eventTime, String method, String url) {
            this.ip = ip;
            this.userId = userId;
            this.eventTime = eventTime;
            this.method = method;
            this.url = url;
        }

        @Override
        public String toString() {
            return "ApacheLogEvent{" +
                    "ip='" + ip + '\'' +
                    ", userId='" + userId + '\'' +
                    ", eventTime=" + eventTime +
                    ", method='" + method + '\'' +
                    ", url='" + url + '\'' +
                    '}';
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public Long getEventTime() {
            return eventTime;
        }

        public void setEventTime(Long eventTime) {
            this.eventTime = eventTime;
        }

        public String getMethod() {
            return method;
        }

        public void setMethod(String method) {
            this.method = method;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

    }

    /**
     * ????????????
     */
    public static class UrlView implements Comparable<UrlView>{
        private String url;
        private long windowEnd;
        private long count;


        @Override
        public String toString() {
            return "UrlView{" +
                    "url='" + url + '\'' +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    ", count=" + count +
                    '}';
        }

        public UrlView(){

        }

        public UrlView(String url, Long windowEnd, Long count) {
            this.url = url;
            this.windowEnd = windowEnd;
            this.count = count;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public Long getWindowEnd() {
            return windowEnd;
        }

        public void setWindowEnd(Long windowEnd) {
            this.windowEnd = windowEnd;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        /**
         * ????????????
         * @param urlView
         * @return
         */
        @Override
        public int compareTo(UrlView urlView) {
            return (this.count > urlView.count) ? -1 : ((this.count == urlView.count) ? 0 : 1);
        }
    }

}
