package example.state.keyed;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;

public class ListStateTest {

    public static void main(String[] args) throws Exception {
        //程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //数据源
        DataStreamSource<Tuple2<Long, Long>> dataStreamSource =
                env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 7L),
                        Tuple2.of(2L, 4L), Tuple2.of(1L, 5L), Tuple2.of(2L, 2L), Tuple2.of(2L, 5L));

        // 输出：
        //(1,5.0)
        //(2,3.6666666666666665)
        dataStreamSource
                .keyBy(0)
                .flatMap(new CountWindowAverageWithListState()) //flatMap,map + state = 自定义函数的感觉
                .print();

        env.execute("TestStatefulApi");
    }
}

class CountWindowAverageWithListState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    /**
     * 1,3
     * 1,7
     * 1,5
     */
    private ListState<Tuple2<Long, Long>> elementsByKey;


    @Override
    public void open(Configuration parameters) throws Exception {

        ListStateDescriptor<Tuple2<Long, Long>> average = new ListStateDescriptor<>(
                "average",
                Types.TUPLE(Types.LONG, Types.LONG)
        );
        elementsByKey = getRuntimeContext().getListState(average);
    }


    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> out) throws Exception {
        Iterable<Tuple2<Long, Long>> currentState = elementsByKey.get();
        if (currentState == null) {
            elementsByKey.addAll(Collections.emptyList());
        }
        elementsByKey.add(element);

        ArrayList<Tuple2<Long, Long>> allElements = Lists.newArrayList(elementsByKey.get());

        if (allElements.size() == 3) {
            long count = 0;
            long sum = 0;
            for (Tuple2<Long, Long> ele : allElements) {
                count++;
                sum += ele.f1;
            }
            double avg = (double) sum / count;
            out.collect(Tuple2.of(element.f0, avg));

            elementsByKey.clear();
        }


    }
}