// package clusterdata.exercise2;

// import clusterdata.datatypes.EventType;
// import clusterdata.datatypes.TaskEvent;
// import clusterdata.sources.TaskEventSource;
// import clusterdata.utils.AppBase;
// import javafx.concurrent.Task;
// import org.apache.flink.api.common.functions.AggregateFunction;
// import org.apache.flink.api.common.state.ValueState;
// import org.apache.flink.api.common.state.ValueStateDescriptor;
// import org.apache.flink.api.java.tuple.*;
// import org.apache.flink.api.java.utils.ParameterTool;
// import org.apache.flink.configuration.Configuration;
// import org.apache.flink.streaming.api.TimeCharacteristic;
// import org.apache.flink.streaming.api.datastream.DataStream;
// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
// import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
// import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
// import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
// import org.apache.flink.streaming.api.windowing.time.Time;
// import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
// import org.apache.flink.util.Collector;
// import scala.Tuple1;
// import scala.concurrent.java8.FuturesConvertersImpl;

// import java.util.*;

// /**
//  * Write a program that identifies every 5 minutes the busiest machines in the cluster during the last 15 minutes.
//  * For every window period, the program must then output the number of busy machines during that last period.
//  *
//  * A machine is considered busy if more than a threshold number of tasks have been scheduled on it
//  * during the specified window period (size).
//  */
// public class BusyMachines extends AppBase {

//     public static void main(String[] args) throws Exception {

//         ParameterTool params = ParameterTool.fromArgs(args);
//         String input = params.get("input", pathToTaskEventData);
//         final int busyThreshold = params.getInt("threshold", 15);

//         final int servingSpeedFactor = 600; // events of 10 minute are served in 1 second

//         // set up streaming execution environment
//         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//         // start the data generator
//         DataStream<TaskEvent> taskEvents = env
//                 .addSource(taskSourceOrTest(new TaskEventSource(input, servingSpeedFactor)))
//                 .setParallelism(1);

//         //TODO: implement the program logic here
//         env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//         DataStream<Tuple2<Long, Integer>> busyMachinesPerWindow = taskEvents
//                 .keyBy("machineId")
//                 .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5)))
//                 .process(new ProcessingFunction(busyThreshold))
//                 .keyBy(0)
//                 .process(new CountFunc());

//         printOrTest(busyMachinesPerWindow);

//         env.execute("Number of busy machines every 5 minutes over the last 15 minutes");
//     }


//     private static class ProcessingFunction extends ProcessWindowFunction<TaskEvent, Tuple2<Long, Long>, Tuple, TimeWindow> {

//         private int threshold;

//         // pass threshold parameter via constructor
//         public ProcessingFunction(int threshold){
//             super();
//             this.threshold = threshold;
//         }

//         @Override
//         public void process(Tuple key, Context context, Iterable<TaskEvent> input, Collector<Tuple2<Long, Long>> out) throws Exception {
//             // count schedule entry for each key
//             int count = 0;
//             for (TaskEvent each: input){
//                 if (each.eventType ==  EventType.SCHEDULE){
//                     count += 1;
//                 }
//             }
//             // output if over threshold
//             if (count >= threshold){
//                 out.collect(new Tuple2(context.window().getEnd(), context.window().getEnd()));
//             }
//         }
//     }

//     private static class CountFunc extends KeyedProcessFunction<Tuple, Tuple2<Long, Long>, Tuple2<Long, Integer>>{
//         // keep <Timestamp, count>
//         Map<Long, Integer> mp = new HashMap<>();

//         @Override
//         public void processElement(Tuple2<Long, Long> input, Context context, Collector<Tuple2<Long, Integer>> collector) throws Exception {
//             // update count
//             if (mp.containsKey(input.f0)) {
//                 mp.put(input.f0, mp.get(input.f0) + 1);
//             }else{
//                 mp.put(input.f0, 1);
//                 // set timer if the timer hasn't been set.
//                 context.timerService().registerEventTimeTimer(input.f0);
//             }
//         }

//         @Override
//         public void onTimer(long timestamp, OnTimerContext context, Collector<Tuple2<Long, Integer>> out) throws Exception {
//             Long key = context.getCurrentKey().getField(0);
//             // output the result
//             out.collect(new Tuple2<>(key, mp.get(key)));

//         }

//     }

// }
