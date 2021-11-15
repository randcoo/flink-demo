package example.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//实时统计单词出现次数
public class WordCount {
    public static void main(String[] args) throws Exception {
        //创建程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //数据的输入
        DataStreamSource<String> myDataStream = env.socketTextStream("localhost", 1234);
        //数据的处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = myDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    out.collect(new Tuple2<>(word, 1));
                }

            }
        }).keyBy(0)
                .sum(1);
        //数据的输出
        result.print();
        //启动应用程序
        env.execute("WordCount");

    }
}
