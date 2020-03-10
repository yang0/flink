/*
 * Copyright 2018 data Artisans GmbH, 2019 Ververica GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flinktraining.exercises.datastream_java.windows;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.utils.MissingSolutionException;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * The "Hourly Tips" exercise of the Flink training
 * (http://training.ververica.com).
 *
 * The task of the exercise is to first calculate the total tips collected by
 * each driver, hour by hour, and then from that stream, find the highest tip
 * total in each hour.
 *
 * Parameters: -input path-to-input-file
 *
 */
public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));
		
		// compute tips per hour for each driver  
		DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
		            // 根据driveId 进行分组  
		            .keyBy((TaxiFare fare) -> fare.driverId)  
		            // 设置窗口时间为1小时  
		            .window(TumblingEventTimeWindows.of(Time.hours(1)))
		            // AddTips()为aggFunction, WrapWithWindowInfo()为windowFunction
		           // 先聚合，再输出，和reduce一样
		            .aggregate(new AddTips(), new WrapWithWindowInfo());
		  
		   // find the highest total tips in each hour  
		   // maxBy(2) 返回包含最大值的元素
		   DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips  
		            .timeWindowAll(Time.hours(1))  
		            .maxBy(2);  
		
		

		

		printOrTest(hourlyMax);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

	private static class AddTips implements AggregateFunction<TaxiFare, Float, Float> {

		@Override
		public Float createAccumulator() {
			return 0f;
		}

		@Override
		public Float add(TaxiFare fare, Float accumulator) {
			return fare.tip + accumulator;
		}

		@Override
		public Float getResult(Float accumulator) {
			return accumulator;
		}

		@Override
		public Float merge(Float a, Float b) {
			return a + b;
		}
	}

	public static class WrapWithWindowInfo
			extends ProcessWindowFunction<Float, Tuple3<Long, Long, Float>, Long, TimeWindow> {

		@Override
		public void process(Long key,
				ProcessWindowFunction<Float, Tuple3<Long, Long, Float>, Long, TimeWindow>.Context context,
				Iterable<Float> elements, Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			
			
			Float sumOfTips = elements.iterator().next();
			out.collect(new Tuple3<>(context.window().getEnd(), key, sumOfTips));
		}
	}

}
