//package example.sink;
//
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
///**
// * 把数据写入redis
// */
//public class SinkForRedisDemo {
//    public static void main(String[] args) throws  Exception {
//        //程序入口
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        //数据源
//        DataStreamSource<String> text = env.socketTextStream("bigdata02", 8888, "\n");
//        //lpsuh l_words word
//        //对数据进行组装,把string转化为tuple2<String,String>
//        DataStream<Tuple2<String, String>> l_wordsData = text.map(new MapFunction<String, Tuple2<String, String>>() {
//            @Override
//            public Tuple2<String, String> map(String value) throws Exception {
//                //k v
//                return new Tuple2<>("f", value);
//            }
//        });
////        //创建redis的配置
//        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("bigdata04").setPort(6379).setPassword("bigdata04").build();
////
////        //创建redissink
//        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(conf, new MyRedisMapper());
//
//
//
//        l_wordsData.addSink(redisSink);
//
//
//        env.execute("StreamingDemoToRedis");
//
//    }
//
//    /**
//     * 把数据插入到redis到逻辑
//     */
//    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {
//        //表示从接收的数据中获取需要操作的redis key
//        @Override
//        public String getKeyFromData(Tuple2<String, String> data) {
//            return data.f0;
//        }
//        //表示从接收的数据中获取需要操作的redis value
//        @Override
//        public String getValueFromData(Tuple2<String, String> data) {
//            return data.f1;
//        }
//
//        @Override
//        public RedisCommandDescription getCommandDescription() {
//            return new RedisCommandDescription(RedisCommand.LPUSH);
//        }
//    }
//}