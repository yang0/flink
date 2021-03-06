package com.ververica.flinktraining.exercises.datastream_java.basics;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.ververica.flinktraining.exercises.datastream_java.basics.RideCleansingExercise.Enrichment;
import com.ververica.flinktraining.exercises.datastream_java.basics.RideCleansingExercise.NYCFilter;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.EnrichedRide;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;

// 测试flatmap
public class FlatMapExercise extends RideCleansingExercise {
	
	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		DataStream<EnrichedRide> enrichedNYCRides = rides
				.flatMap(new NYCEnrichment());
		
		enrichedNYCRides.print();

		// run the cleansing pipeline
		env.execute("Taxi Ride Cleansing");
	}
	
	public static class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {
		  @Override
		  public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
		    FilterFunction<TaxiRide> valid = new NYCFilter();
		    if (valid.filter(taxiRide)) {
		      out.collect(new EnrichedRide(taxiRide));
		    }
		  }
		}

}
