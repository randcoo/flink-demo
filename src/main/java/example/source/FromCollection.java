package example.source;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FromCollection {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream dataStream = environment.fromCollection(Lists.newArrayList(1, 2, 3, 4, 5));
		dataStream.print();
		environment.execute(FromCollection.class.getSimpleName());
	}
}
