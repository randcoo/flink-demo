package example.operator;

import example.MyNoParallelSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 *  根据规则把一个数据流切分为多个流
 应用场景：
 * 可能在实际工作中，源数据流中混合了多种类似的数据，多种类型的数据处理规则不一样，所以就可以在根据一定的规则，
 * 把一个数据流切分成多个数据流，这样每个数据流就可以使用不用的处理逻辑了
 */
public class SplitOperator {

    public static void main(String[] args) throws  Exception {
        //获取Flink的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<Long> text = env.addSource(new MyNoParallelSource()).setParallelism(1);//注意：针对此source，并行度只能设置为1

        // 使用Side Output分流
        final OutputTag<Long> even = new OutputTag<Long>("even"){};
        final OutputTag<Long> odd = new OutputTag<Long>("odd"){};
        //对流进行分流，按照数据的奇偶性进行区分
        SingleOutputStreamOperator<Long> singleOutputStreamOperator = text.process(new ProcessFunction<Long, Long>() {
            @Override
            public void processElement(Long value, Context context, Collector<Long> collector) throws Exception {
                if (value % 2 == 0) {
                    context.output(even, value);//偶数
                } else {
                    context.output(odd, value);//奇数
                }
            }
        });

        DataStream<Long> eventStream = singleOutputStreamOperator.getSideOutput(even);
        DataStream<Long> oddStream = singleOutputStreamOperator.getSideOutput(odd);

        //选择一个或者多个切分后的流
        //打印结果
        //打印偶数
        eventStream.print().setParallelism(1);
        //打印奇数
//        oddStream.print().setParallelism(1);
        //打印全部
        String jobName = SplitOperator.class.getSimpleName();
        env.execute(jobName);

    }
}