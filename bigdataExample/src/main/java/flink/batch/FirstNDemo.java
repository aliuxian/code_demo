package flink.batch;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

public class FirstNDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> datas = new ArrayList<>();
        datas.add(Tuple2.of(2, "zs"));
        datas.add(Tuple2.of(4, "ls"));
        datas.add(Tuple2.of(3, "ww"));
        datas.add(Tuple2.of(2, "zl"));
        datas.add(Tuple2.of(9, "sq"));
        datas.add(Tuple2.of(9, "wb"));
        datas.add(Tuple2.of(7, "ly"));
        datas.add(Tuple2.of(8, "ce"));

        DataSource<Tuple2<Integer, String>> tuple2DataSource = env.fromCollection(datas);

        System.out.println("前三条数据，插入顺序");
        tuple2DataSource.first(3).print();

        System.out.println("分组求每组前两个元素");
        tuple2DataSource.groupBy(0).first(2).print();

        System.out.println("分组，组内排序，取每组的第一个元素");
        tuple2DataSource.groupBy(0).sortGroup(1, Order.ASCENDING).first(1).print();

        System.out.println("全局多字段排序");
        tuple2DataSource.sortPartition(0, Order.ASCENDING).sortPartition(1, Order.DESCENDING).first(3).print();


    }
}
