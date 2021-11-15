package example.state.keyed;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *  需求：当接收到的相同 key 的元素个数等于 3 个
 *  就计算这些元素的 value 的平均值。
 *  计算 keyed stream 中每 3 个元素的 value 的平均值
 *
 *  1，3
 *  1，7
 *
 *  1，5
 *
 *  1，5.0
 *
 *  2，4
 *
 *  2，2
 *  2，5
 *
 *  2，3.666
 *
 *  key,value
 *  1 long,5 doulbe
 *
 */
public class ValueStateTest {
    public static void main(String[] args) throws  Exception{
        //程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据源
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(1L, 5L),Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));

        // 输出：
        //(1,5.0)
        //(2,3.6666666666666665)
        dataStreamSource
                .keyBy(0)
                .flatMap(new CountWindowAverageWithValueState()) //flatMap,map + state = 自定义函数的感觉
                .print();

        env.execute("TestStatefulApi");
    }


}


class CountWindowAverageWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    /**
     * 1.valueState 属于keyed state
     * 2.valueState里面只能存储一条数据
     *
     * 思路:
     * long1:当前key出现的次数
     * long2:累加的value值
     * if(long1=3){
     * long2/long1 =avg
     * }
     */

    private ValueState<Tuple2<Long, Long>> countAndSum;

    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<Tuple2<Long, Long>> average = new ValueStateDescriptor<>(
                "average",
                Types.TUPLE(Types.LONG, Types.LONG)
        );

        countAndSum = getRuntimeContext().getState(average);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> out) throws Exception {

        Tuple2<Long, Long> currentState = countAndSum.value();
        if (currentState == null) {
            currentState = Tuple2.of(0L, 0L);
        }
        //统计key出现的次数
        currentState.f0 += 1;
        //统计value总值
        currentState.f1 += element.f1;
        countAndSum.update(currentState);

        if (currentState.f0 ==3){
            double avg =(double)currentState.f1/currentState.f0;
            out.collect(Tuple2.of(element.f0,avg));
            //清空里面的数据
            countAndSum.clear();
        }
    }
}