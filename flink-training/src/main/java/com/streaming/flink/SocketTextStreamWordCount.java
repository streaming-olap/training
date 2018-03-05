package com.streaming.flink;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

/**
 * This example shows an implementation of WordCount with data from a text
 * socket. To run the example make sure that the service providing the text data
 * is already up and running.
 *
 * <p>To start an example socket text stream on your local machine run netcat from
 * a command line: <code>nc -lk 9999</code>, where the parameter specifies the
 * port number.
 *
 * <p>Usage:
 * <code>SocketTextStreamWordCount &lt;hostname&gt; &lt;port&gt;</code>
 * <br>
 *
 * <p>This example shows how to:
 * <ul>
 * <li>use StreamExecutionEnvironment.socketTextStream
 * <li>write a simple Flink program
 * <li>write and use user-defined functions
 * </ul>
 *
 * @see <a href="www.openbsd.org/cgi-bin/man.cgi?query=nc">netcat</a>
 */
public class SocketTextStreamWordCount {

	//
	//	Program
	//

	public static void main(String[] args) throws Exception {

		String hostName = "localhost";
		Integer port = 8000;


		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment().setParallelism(1);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// get input data
		DataStream<String> text = env.socketTextStream(hostName, port);

		DataStream<Tuple2<String, String>> counts =
				text.flatMap(new LineSplitter()).setParallelism(1)
						.assignTimestampsAndWatermarks(new BoundedWaterMark(60))
						.keyBy(new KeySelector<HashMap<String,String>, Object>() {
							@Override
							public Object getKey(HashMap<String, String> map) throws Exception {
								return map.get("uid");
							}
						})
						.window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                              .allowedLateness(Time.milliseconds(10000))
						.apply(
								new WindowFunction<HashMap<String,String>, Tuple2<String,String>, Object, TimeWindow>() {
									@Override
									public void apply(Object o, TimeWindow timeWindow, Iterable<HashMap<String, String>> iterable, Collector<Tuple2<String, String>> collector) throws Exception {
										System.out.println("this window is fired:" + timeWindow);
										for(HashMap map : iterable){
											System.out.println("this fired has element:" + map.get("timestamp"));
										}
									}
								}).setParallelism(1);

		// execute program
		env.execute("Window example");
	}

	//
	// 	User Functions
	//

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
