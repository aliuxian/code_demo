package flink.batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

public class AccumulatorDemo {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("a", "b", "c", "d", "e", "f", "g", "h");

        MapOperator<String, String> result = data.map(new RichMapFunction<String, String>() {


            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }

            @Override
            public String map(String s) throws Exception {
                this.numLines.add(1);
                // 如果在这里获取累加器的值
                // 并行度为1的情况下，该累加器的结果才正确，多并行度下结果会出错。
                System.out.println(numLines.getLocalValue() + "***");
                return s;
            }
        }).setParallelism(3);

//        result.print();


        result.writeAsText("/tmp/output", FileSystem.WriteMode.OVERWRITE);

        // 累加器的结果存在JobExecutionResult中，而JobExecutionResult是execute方法返回的，所以必须要等待作业执行完成才可以
        JobExecutionResult jobResult = env.execute("ACC");
        int accumulatorResult = jobResult.getAccumulatorResult("num-lines");

        System.out.println(accumulatorResult);


    }
}
