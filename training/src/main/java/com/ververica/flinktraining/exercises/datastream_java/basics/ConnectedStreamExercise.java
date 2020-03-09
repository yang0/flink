package com.ververica.flinktraining.exercises.datastream_java.basics;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

// 动态控制数据转换
// 创建布尔值，把布尔值存到key中，这个key是两个stream共享的，相关的值也是共享的
public class ConnectedStreamExercise {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

	    // control流用于指定必须从streamOfWords流中过滤掉的单词
	    DataStream<String> control = env.fromElements("DROP", "IGNORE").keyBy(x -> x);
	    // data和artisans不在control流中，状态是不会被记录为true的（flatMap1），即为null，所以streamOfWords在调用flatMap2时会被out输出
	    DataStream<String> streamOfWords = env.fromElements("data", "DROP", "artisans", "IGNORE").keyBy(x -> x);

	    // 两个流要想被连接在一块，要么两个流都是未分组的，要么都是分组的即keyed-都做了keyby操作；如果都做了keyby，「key的值必须是相同的」
	    control.connect(streamOfWords)
	            .flatMap(new ControlFunction())
	            .print();

	    /**
	     * 1> data
	     * 4> artisans
	     */
   
	    env.execute("ConnectedStreamsExerise Job");

	}
	
	
	public static class ControlFunction extends RichCoFlatMapFunction<String, String, String> {
		private ValueState<Boolean> blocked;
			
		@Override
		public void open(Configuration config) {
		    blocked = getRuntimeContext().getState(new ValueStateDescriptor<>("blocked", Boolean.class));
		}
			
		// 输入的是control流
		@Override
		public void flatMap1(String control_value, Collector<String> out) throws Exception {
		    blocked.update(Boolean.TRUE);
		}
			
		// 输入的是streamOfWords流
		@Override
		public void flatMap2(String data_value, Collector<String> out) throws Exception {
		    if (blocked.value() == null) {
			    out.collect(data_value);
			}
		}
	}

}



