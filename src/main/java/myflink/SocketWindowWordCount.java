package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @CopyRightBy zcy.qyg.cn
 * @Author DongQing
 * @Date 2019-06-20 23:01
 * @ClassName SocketWindowWordCount
 * @Description TODO
 **/
public class SocketWindowWordCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost",9000,"\n");
        DataStream<Tuple2<String, Integer>> windowCounts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                for (String word : value.split("\\s")) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);
        // 将结果打印到控制台，注意这里使用的是单线程打印，而非多线程  为了打印有序
        windowCounts.print().setParallelism(1);
        env.execute("Socket Window WordCount");
    }
}
