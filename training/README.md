This repository contains examples as well as reference solutions and utility classes for the Apache Flink Training exercises 
found on [http://training.ververica.com](http://training.ververica.com).

## 练习
* https://training.ververica.com/
* package: ..exercise.datastream_java.basics
1. filter, map
2. flat map
3. keyed by
4. Stateful Transformations
5. Connected Stream

com.ververica.flinktraining.exercises.datastream_java.state.RidesAndFaresExercise

6. Stateful Enrichment
7. Window: com.ververica.flinktraining.exercises.datastream_java.windows.HourlyTipsExercise。尝试了process, aggregate, reduce等windowFunction
8. Timer, Watermarks: com.ververica.flinktraining.examples.datastream_java.process.CarEventSort
9. side outpu： 没有实际练习代码，文档已相当清晰
10. Event-time timer: com.ververica.flinktraining.exercises.datastream_java.process.ExpiringStateExercise
	* checkpointed sources 
	* Timer
	* CoProcessFunction 