package example.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FromElement {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream dataStream = environment.fromElements(1, 2, 3, 4, 5);
		dataStream.print();
		environment.execute(FromElement.class.getSimpleName());
	}
}
