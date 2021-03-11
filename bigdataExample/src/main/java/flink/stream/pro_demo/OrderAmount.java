package flink.stream.pro_demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OrderAmount {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.readTextFile("data/order_test")
//                .map()

    }




}
