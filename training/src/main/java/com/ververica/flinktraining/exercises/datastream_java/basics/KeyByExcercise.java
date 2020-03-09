package com.ververica.flinktraining.exercises.datastream_java.basics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.Interval;
import org.joda.time.Minutes;

import com.ververica.flinktraining.exercises.datastream_java.basics.RideCleansingExercise.Enrichment;
import com.ververica.flinktraining.exercises.datastream_java.basics.RideCleansingExercise.NYCFilter;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.EnrichedRide;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;

// 练习keyby功能
// 获取每个cell坐车时间最长的记录
// https://training.ververica.com/lessons/keyed-streams.html
public class KeyByExcercise extends FlatMapExercise {

	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);

		final int maxEventDelay = 60; // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiRide> rides = env
				.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		// 第一步，根据经纬度加入cell信息
		DataStream<EnrichedRide> enrichedNYCRides = rides.flatMap(new NYCEnrichment());

//		 第二步，转换成Tuple2格式，只保留聚合需要用的startcell，以及duration两个数据
		DataStream<Tuple2<Integer, Minutes>> minutesByStartCell = enrichedNYCRides
				.flatMap(new FlatMapFunction<EnrichedRide, Tuple2<Integer, Minutes>>() {
					@Override
					public void flatMap(EnrichedRide ride, Collector<Tuple2<Integer, Minutes>> out) throws Exception {
						if (!ride.isStart) {
							Interval rideInterval = new Interval(ride.startTime, ride.endTime);
							Minutes duration = rideInterval.toDuration().toStandardMinutes();
							out.collect(new Tuple2<>(ride.startCell, duration));
						}
					}
				});
		
//		第三步, 聚合统计
		minutesByStartCell
	    .keyBy(0) // startCell
	    .maxBy(1) // duration
	    .print();


		env.execute("Taxi Ride Cleansing");
	}

}
