/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package myflink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {


	public static void main(String[] args) throws Exception {
		//定义socket的端口号
       /* int port;
        try{
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        }catch (Exception e){
            System.err.println("没有指定port参数，使用默认值9000");
            port = 9000;
        }*/

		//获取运行环境
		//StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamExecutionEnvironment env = LocalStreamEnvironment.getExecutionEnvironment();

		//连接socket获取输入的数据
		//DataStreamSource<String> text = env.socketTextStream("10.192.12.106", port, "\n");
		DataStreamSource<String> text = env.socketTextStream("127.0.0.1", 9000, "\n");

		//计算数据
		DataStream<WordWithCount> windowCount = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
			public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
				String[] splits = value.split("\\s");
				for (String word:splits) {
					out.collect(new WordWithCount(word,1L));
				}
			}
		})//打平操作，把每行的单词转为<word,count>类型的数据
				.keyBy("word")//针对相同的word数据进行分组
				.timeWindow(Time.seconds(10),Time.seconds(10))//指定计算数据的窗口大小和滑动窗口大小
				.sum("count");

		//把数据打印到控制台
		windowCount.print()
				.setParallelism(1);//使用一个并行度
		//注意：因为flink是懒加载的，所以必须调用execute方法，上面的代码才会执行
		env.execute("streaming word count");

	}

	/**
	 * 主要为了存储单词以及单词出现的次数
	 */
	public static class WordWithCount{
		public String word;
		public long count;
		public WordWithCount(){}
		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return "WordWithCount{" +
					"word='" + word + '\'' +
					", count=" + count +
					'}';
		}
	}


}
