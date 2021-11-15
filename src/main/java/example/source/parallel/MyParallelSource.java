package example.source.parallel;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * 我们的这个source是支持多并行度的
 */
public class MyParallelSource implements ParallelSourceFunction<Long> {
    private long number = 1L;
    private boolean isRunning = true;
    @Override
    public void run(SourceContext<Long> sct) throws Exception {
        while (isRunning){
            sct.collect(number);
            number++;
            //每秒生成一条数据
            Thread.sleep(60000);
        }

    }

    @Override
    public void cancel() {
        isRunning=false;
    }
}
