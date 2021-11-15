package example.state.keyed;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class AggregationStateTest {

    public static void main(String[] args) throws  Exception{
        //程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据源
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(1L, 5L),Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));

        // 输出：
        dataStreamSource
                .keyBy(0)
                .flatMap(new ContainsValueFunction()) //flatMap,map + state = 自定义函数的感觉
                .print();

        env.execute("TestStatefulApi");
    }
}

class ContainsValueFunction
        extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, String>> {
    /**
     * 1, contains:3 and 5
     */
    private AggregatingState<Long, String> totalStr;//辅助字段

    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        AggregatingStateDescriptor<Long, String, String> descriptor =
                new AggregatingStateDescriptor<Long, String, String>(
                        "totalStr",  // 状态的名字

                        //SparkSQL 自定义聚合函数
                        new AggregateFunction<Long, String, String>() {
                            //初始化的操作，只运行一次哦
                            @Override
                            public String createAccumulator() {
                                return "Contains：";
                            }

                            @Override
                            public String add(Long value, String accumulator) {
                                if ("Contains：".equals(accumulator)) {
                                    return accumulator + value;
                                }
                                return accumulator + " and " + value;
                            }

                            @Override
                            public String merge(String a, String b) {
                                return a + " and " + b;
                            }

                            @Override
                            public String getResult(String accumulator) {
                                //contains:1
                                //contains: 1 and 3 and
                                return accumulator;
                            }
                        }, String.class); // 状态存储的数据类型
        totalStr = getRuntimeContext().getAggregatingState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element,
                        Collector<Tuple2<Long, String>> out) throws Exception {
        totalStr.add(element.f1);

        out.collect(Tuple2.of(element.f0, totalStr.get()));
    }
}