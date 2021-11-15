package example.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * state:
 * keyed
 * operator  -> checkpoint
 */
public class WordCount4 {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostname = parameterTool.get("hostname");
        int port = parameterTool.getInt("port");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //10s 15s
        //如果数据量比较大，建议5分钟左右checkpoint的一次。
        //阿里他们使用的时候 也是这样建议的。
        env.enableCheckpointing(10000);//10s 15s state

        FsStateBackend fsStateBackend = new FsStateBackend("hdfs://bigdata02:9000/flink_1/checkpoint");

        MemoryStateBackend memoryStateBackend = new MemoryStateBackend();

        env.setStateBackend(fsStateBackend);

        env.setStateBackend(new RocksDBStateBackend("hdfs://bigdata02:9000/flink_2/checkpoint"));

        //setCheckpointingMode---是否允许数据重复
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //setMinPauseBetweenCheckpoints  ---两个checkpoint之间间隔多久
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        //setCheckpointTimeout ---超时时间 
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        //enableExternalizedCheckpoints---cancel程序的时候保存checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启的次数
                Time.of(10, TimeUnit.SECONDS) // 间隔
        ));

        DataStreamSource<String> dataStream = env.socketTextStream(hostname, port);
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(0)
                .sum(1);

        result.print();

        env.execute("WordCount check point....");
    }
}