package com.ververica.flinktraining.exercises.datastream_java.basics;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

import java.util.LinkedList;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.ververica.flinktraining.exercises.datastream_java.basics.RideCleansingExercise.Enrichment;
import com.ververica.flinktraining.exercises.datastream_java.basics.RideCleansingExercise.NYCFilter;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.EnrichedRide;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;

// 运行本例需要启动nc -l 19000找个服务
// 计算平均数
public class StateExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<String> text = env.socketTextStream("localhost", 19000, "\n");
		DataStream<Tuple2<String, Double>> input = text.map(new MapFunction<String, Tuple2<String, Double>>() {

			@Override
			public Tuple2<String, Double> map(String s) throws Exception {
				// S001 2.0
				// S001 5.0
				// S001 9.0
				// nc -l 19000
				String[] record = s.split("\\W+");
				return new Tuple2<>(record[0], Double.valueOf(record[1]));
			}

		});
		
		DataStream<Tuple2<String, Double>> smoothed = input.keyBy(0).map(new Smoother());
		
		smoothed.print();

		// run the cleansing pipeline
		env.execute("MovingAverageKeyedStateExercise Job");
	}

	// 实现一个有状态的转换RichMapFunction。

	public static class Smoother extends RichMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {
		private ValueState<MovingAverage> averageState;

		@Override
		public void open(Configuration parameters) throws Exception {
			ValueStateDescriptor<MovingAverage> descriptor = new ValueStateDescriptor<>("moving average",
					MovingAverage.class);
			averageState = getRuntimeContext().getState(descriptor);
		}

		@Override
		public Tuple2<String, Double> map(Tuple2<String, Double> item) throws Exception {
			MovingAverage average = averageState.value();
			if (average == null) {
				average = new MovingAverage(2);
			}

			average.add(item.f1);
			averageState.update(average);

			return new Tuple2(item.f0, average.getAverage());
		}

	}

}

class MovingAverage {
	double sum;
	int size;
	LinkedList<Double> list;
	double average;

	public MovingAverage(int size) {
		this.list = new LinkedList<Double>();
		this.size = size;
	}

//	和前几个值一起求个平均数
	public double add(double val) {
		sum += val;
		list.offer(val);

		if (list.size() <= size) {
			return sum / list.size();
		}

		sum -= list.poll();
		return average = sum / size;
	}

	/**
	 * @return the average
	 */
	public double getAverage() {
		return average;
	}

}
