package com.chenxiang.flink.practice;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
public class SocketTextStreamWordCount {

    public static void main(String[] args) throws Exception {

        String hostName = "localhost";
        Integer port = 9000;


        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // get input data
        DataStream<String> text = env.socketTextStream(hostName, port);

        DataStream<Tuple2<String, String>> counts =
                text.flatMap(new LineSplitter()).setParallelism(1)
                        .assignTimestampsAndWatermarks(new BoundedWaterMark(60))
                        .keyBy(new KeySelector<HashMap<String, String>, Object>() {
                            @Override
                            public Object getKey(HashMap<String, String> map) throws Exception {
                                return map.get("uid");
                            }
                        })
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                      .allowedLateness(Time.milliseconds(10000))
                        .apply(new WindowFunction<HashMap<String, String>, Tuple2<String, String>, Object, TimeWindow>() {
                            @Override
                            public void apply(Object o, TimeWindow timeWindow, Iterable<HashMap<String, String>> iterable, Collector<Tuple2<String, String>> collector) throws Exception {
                                System.out.println("this window is fired:" + timeWindow);
                                for (HashMap map : iterable) {
                                    System.out.println("this fired has element:" + map.get("timestamp"));
                                }
                            }
                        }).setParallelism(1);

        // execute program
        env.execute("Window example");
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" (Tuple2&lt;String, Integer&gt;).
     */
    public static final class LineSplitter implements FlatMapFunction<String, HashMap<String, String>> {

        @Override
        public void flatMap(String value, Collector<HashMap<String, String>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");
            HashMap<String, String> res = new HashMap<>();
            res.put("uid", tokens[0]);
            res.put("timestamp", tokens[1]);
            res.put("data", tokens[2]);
            out.collect(res);
        }
    }
}
