package example.operator;

import example.MyNoParallelSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 和union类似，但是只能连接两个流，两个流的数据类型可以不同，会对两个流中的数据应用不同的处理方法
 */
public class ConnectOperator {

    public static void main(String[] args) throws Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataStreamSource<Long> streamSource1 = env.addSource(new MyNoParallelSource()).setParallelism(1);//注意：针对此source，并行度只能设置为1

        DataStreamSource<Long> streamSource2 = env.addSource(new MyNoParallelSource()).setParallelism(1);


        SingleOutputStreamOperator<String> streamSource2Strs = streamSource2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "streamSource2_" + value;
            }
        });

        //union
        ConnectedStreams<Long, String> connectStream = streamSource1.connect(streamSource2Strs);


        SingleOutputStreamOperator<Object> result = connectStream.map(new CoMapFunction<Long, String, Object>() {
            //这个方法处理的是数据源 1
            @Override
            public Object map1(Long value) throws Exception {
                return "1-->" + value;
            }

            //这个方法处理的就是数据源 2
            @Override
            public Object map2(String value) throws Exception {
                return "2-->" + value;
            }
        });

        //打印结果
        result.print().setParallelism(1);
        String jobName = ConnectOperator.class.getSimpleName();
        env.execute(jobName);
    }
}
