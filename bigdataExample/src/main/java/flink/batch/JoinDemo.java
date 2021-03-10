package flink.batch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

public class JoinDemo {
    public static void main(String[] args) throws  Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //tuple2<用户id，用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"zs"));
        data1.add(new Tuple2<>(2,"ls"));
        data1.add(new Tuple2<>(3,"ww"));

        //tuple2<用户id，用户所在城市>
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"beijing"));
        data2.add(new Tuple2<>(2,"shanghai"));
        data2.add(new Tuple2<>(4,"guangzhou"));

        DataSource<Tuple2<Integer, String>> dataSet1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> dataSet2 = env.fromCollection(data2);


        //dataset1 join dataset2
        // key     join   key
        dataSet1.join(dataSet2)
                .where(0) // dataset1 key
                .equalTo(0) //dataset2 key
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
                    @Override
                    public Tuple3<Integer,String,String> join(Tuple2<Integer, String> d1, Tuple2<Integer, String> d2) throws Exception {
                        //你可以自己实现一些更复杂的逻辑
                        return new Tuple3<>(d1.f0,d1.f1,d2.f1);
                    }
                }).print();
    }
}
