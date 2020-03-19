// package clusterdata.exercise2;

// import clusterdata.datatypes.EventType;
// import clusterdata.datatypes.TaskEvent;
// import clusterdata.sources.TaskEventSource;
// import clusterdata.utils.AppBase;
// import org.apache.flink.api.common.functions.AggregateFunction;
// import org.apache.flink.api.common.functions.FilterFunction;
// import org.apache.flink.api.java.tuple.Tuple;
// import org.apache.flink.api.java.tuple.Tuple3;
// import org.apache.flink.api.java.tuple.Tuple4;
// import org.apache.flink.api.java.tuple.Tuple5;
// import org.apache.flink.api.java.utils.ParameterTool;
// import org.apache.flink.streaming.api.TimeCharacteristic;
// import org.apache.flink.streaming.api.datastream.DataStream;
// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
// import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
// import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
// import org.apache.flink.streaming.api.windowing.time.Time;
// import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
// import org.apache.flink.util.Collector;

// /**
//  * Count successful, failed, and killed tasks per machine per minute.
//  */
// public class PerMachineTaskStatistics extends AppBase {

//     public static void main(String[] args) throws Exception {

//         ParameterTool params = ParameterTool.fromArgs(args);
//         String input = params.get("input", pathToTaskEventData);

//         final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

//         // set up streaming execution environment
//         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//         // start the data generator
//         DataStream<TaskEvent> taskEvents = env
//                 .addSource(taskSourceOrTest(new TaskEventSource(input, servingSpeedFactor)))
//                 .setParallelism(1);

//         //TODO: implement the window logic here
//         env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//          DataStream<Tuple5<Long, Long, Integer, Integer, Integer>> statistics = taskEvents.filter(new FilterFunction<TaskEvent>(){
//              @Override
//              public boolean filter(TaskEvent taskEvent) throws Exception {
//                  return (taskEvent.eventType == EventType.FINISH || taskEvent.eventType == EventType.FAIL || taskEvent.eventType == EventType.KILL );
//              }
//          })
//                  // different from the first one, we need to key by machineId in this case. Others stay the same.
//                  .keyBy("machineId").window(TumblingEventTimeWindows.of(Time.minutes(1))).aggregate(new eventTypeAggregate(), new startTimeProcessWindowFunction());
//         printOrTest(statistics);

//         env.execute("Per machine task statistics");
//     }

//     private static final class eventTypeAggregate implements AggregateFunction<TaskEvent, Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> {

//         @Override
//         public Tuple3<Integer, Integer, Integer> createAccumulator() {
//             return new Tuple3<>(0, 0, 0);
//         }

//         @Override
//         public Tuple3<Integer, Integer, Integer> add(TaskEvent taskEvent, Tuple3<Integer, Integer, Integer> acc) {
//             Tuple3<Integer, Integer, Integer> result;
//             if (taskEvent.eventType == EventType.FINISH){
//                 result = new Tuple3(acc.f0 + 1, acc.f1, acc.f2);
//             }
//             else if(taskEvent.eventType == EventType.FAIL){
//                 result = new Tuple3(acc.f0, acc.f1 + 1, acc.f2);
//             }
//             else if(taskEvent.eventType == EventType.KILL){
//                 result = new Tuple3(acc.f0, acc.f1, acc.f2 + 1);
//             }
//             else{
//                 result = acc;
//             }
//             return result;
//         }

//         @Override
//         public Tuple3<Integer, Integer, Integer> getResult(Tuple3<Integer, Integer, Integer> acc) {
//             return acc;
//         }

//         @Override
//         public Tuple3<Integer, Integer, Integer> merge(Tuple3<Integer, Integer, Integer> t1, Tuple3<Integer, Integer, Integer> t2) {
//             return new Tuple3<>(t1.f0 + t2.f0, t1.f1 + t2.f1, t1.f2 + t2.f2);
//         }

//     }

//     private static final class startTimeProcessWindowFunction extends ProcessWindowFunction<Tuple3<Integer, Integer, Integer>, Tuple5<Long, Long, Integer, Integer, Integer>, Tuple, TimeWindow> {

//         @Override
//         public void process(Tuple tuple, Context context, Iterable<Tuple3<Integer, Integer, Integer>> iterable, Collector<Tuple5<Long, Long, Integer, Integer, Integer>> collector) throws Exception {
//             Tuple3<Integer, Integer, Integer> result = iterable.iterator().next();
//             // get window start time from context
//             collector.collect(new Tuple5(context.window().getStart(), tuple.getField(0), result.f0, result.f1, result.f2));
//         }
//     }
// }
