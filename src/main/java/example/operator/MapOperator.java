package example.operator;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapOperator {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.fromElements(1, 2, 3, 4, 5)
			.map(item -> item * 2)
			.print();

		env.execute(MapOperator.class.getSimpleName());

	}
}
