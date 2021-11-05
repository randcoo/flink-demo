package example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingDemoWithMyNoPralalleSource {
    public static void main(String[] args) throws Exception {

        /**
         * 1. 获取程序入口
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());


        /**
         * 2 获取数据源
         */

        DataStreamSource<Long> numberStream = env.addSource(new MyNoParalleSource()).setParallelism(1);

        /**
         * 3 数据的处理
         */
        SingleOutputStreamOperator<Long> dataStream = numberStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接受到了数据："+value);
                return value;
            }
        }).setParallelism(2);

        SingleOutputStreamOperator<Long> filterDataStream = dataStream.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long number) throws Exception {
                //过滤出来偶数
                return number % 2 == 0;
            }
        }).setParallelism(2);

        filterDataStream.print().setParallelism(20);

        env.execute("StreamingDemoWithMyNoPralalleSource");
    }
}