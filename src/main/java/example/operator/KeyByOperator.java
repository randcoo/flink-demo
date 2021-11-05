package example.operator;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class KeyByOperator {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		SingleOutputStreamOperator<Long> process = env.fromElements(Long.class, 1L, 2L, 3L, 4L, 100L)
			// .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps())
			.keyBy(new KeySelector<Long, String>() {
				@Override
				public String getKey(Long value) throws Exception {
					if (value % 2 == 0) {
						return "even";
					} else {
						return "odd";
					}
				}
			}).sum("even");
			// .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
			// .process(new ProcessWindowFunction<Long, Long, String, TimeWindow>() {
			// 	@Override
			// 	public void process(String s, Context context, Iterable<Long> elements, Collector<Long> out) throws
			// 		Exception {
			// 		long sum = 0;
			// 		for (Long element : elements) {
			// 			sum += element;
			// 		}
			// 		out.collect(sum);
			//
			// 		if (sum % 2 == 0) {
			// 			context.output(new OutputTag<>("even"), sum);
			// 		} else {
			// 			context.output(new OutputTag<>("odd"), sum);
			// 		}
			//
			// 	}
			// });
		// process.getSideOutput(new OutputTag<>("even")).print();
		// process.getSideOutput(new OutputTag<>("odd")).print();
		env.execute(KeyByOperator.class.getSimpleName());
	}
}
