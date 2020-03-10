package com.ververica.flinktraining.exercises.datastream_java.windows;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.ververica.flinktraining.exercises.datastream_java.windows.HourlyTipsExercise.WrapWithWindowInfo;

// 这个demo只用proccessWindowFunction
public class HourlyTipsExercise2 extends ExerciseBase {
	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60; // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiFare> fares = env
				.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

		// compute tips per hour for each driver
		DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
				// 根据driveId 进行分组
				.keyBy((TaxiFare fare) -> fare.driverId)
				// 设置窗口时间为1小时
				.timeWindow(Time.hours(1))
				// AddTips()为aggFunction, WrapWithWindowInfo()为windowFunction
				// 先聚合，再输出，和reduce一样
				.process(new AddTips());

		// find the highest total tips in each hour
		// maxBy(2) 返回包含最大值的元素
		DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips.timeWindowAll(Time.hours(1)).maxBy(2);

		printOrTest(hourlyMax);

		// execute the transformation pipeline
		env.execute("Hourly Tips (java)");
	}

	public static class AddTips extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
		@Override
		public void process(Long key, Context context, Iterable<TaxiFare> fares,
				Collector<Tuple3<Long, Long, Float>> out) throws Exception {
			Float sumOfTips = 0F;
			for (TaxiFare f : fares) {
				sumOfTips += f.tip;
			}
			out.collect(new Tuple3<>(context.window().getEnd(), key, sumOfTips));
		}
	}
}
