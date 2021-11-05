package example.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlatMapOperator {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.fromElements(1, 2, 3, 4, 5)
			// .flatMap(item -> item * 2)
			.print();

		env.execute(FlatMapOperator.class.getSimpleName());
	}
}
