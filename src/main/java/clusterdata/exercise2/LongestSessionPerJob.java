package clusterdata.exercise2;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.JobEvent;
import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.JobEventSource;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import javafx.concurrent.Task;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.tasks.SystemProcessingTimeService;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Write a program that identifies job stages by their sessions.
 * We assume that a job will execute its tasks in stages and that a stage has finished after an inactivity period of 10 minutes.
 * The program must output the longest session per job, after the job has finished.
 */
public class LongestSessionPerJob extends AppBase {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String taskInput = params.get("task_input", pathToTaskEventData);
        String jobInput = params.get("job_input", pathToJobEventData);

        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);

        // start the data generator
        DataStream<TaskEvent> taskEvents = env
                .addSource(taskSourceOrTest(new TaskEventSource(taskInput, servingSpeedFactor)))
                .setParallelism(1);

        //TODO: implement the program logic here

        // set up jobEvents input
        DataStream<JobEvent> jobEvents = env
                .addSource(jobSourceOrTest(new JobEventSource(jobInput, servingSpeedFactor)))
                .setParallelism(1);

        DataStream<Tuple2<Long, Integer>> maxSessionPerJob = taskEvents.filter(new FilterFunction<TaskEvent>() {
            @Override
            public boolean filter(TaskEvent taskEvent) throws Exception {
                return taskEvent.eventType == EventType.SUBMIT;
            }
        })
                .keyBy("jobId")
                // use SessionWindow to solve this inactivity problem
                .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
                .process(new SessionWindowFunction())
                .connect(jobEvents)
                // after connect to jobEvents, we use a <JobId, type, count> as uniform stream entry, where type shows whether it is a job event or task event.
                .flatMap(new CoFlatMapFunction<Tuple2<Long, Integer>, JobEvent, Tuple3<Long, Integer, Integer>>() {

                    @Override
                    public void flatMap1(Tuple2<Long, Integer> input, Collector<Tuple3<Long, Integer, Integer>> out) throws Exception {
                        out.collect(new Tuple3<>(input.f0, 1, input.f1));
                    }

                    @Override
                    public void flatMap2(JobEvent input, Collector<Tuple3<Long, Integer, Integer>> out) throws Exception {
                        // we only use the finish event, which marks job finished.
                        if (input.eventType == EventType.FINISH){
                            out.collect(new Tuple3<>(input.jobId, 0, 0));
                        }
                    }

                })
                .keyBy(0)
                .process(new LongestSessionFunc());
        printOrTest(maxSessionPerJob);

        env.execute("Maximum stage size per job");
    }

    private static class SessionWindowFunction
            extends ProcessWindowFunction<TaskEvent, Tuple2<Long, Integer>, Tuple, TimeWindow> {

        // calculate task count for each session.
        @Override
        public void process(Tuple key, Context context, Iterable<TaskEvent> input, Collector<Tuple2<Long, Integer>> out) throws Exception {
            int count = 0;
            for(TaskEvent each: input){
                count += 1;
            }
            out.collect(new Tuple2<>(key.getField(0), count));
        }
    }

    private static class LongestSessionFunc extends KeyedProcessFunction<Tuple, Tuple3<Long, Integer, Integer>, Tuple2<Long, Integer>>{

        // this map <JobId, count> is used to keep the maximum session length for each job.
        Map<Long, Integer> mp = new HashMap<>();

        @Override
        public void processElement(Tuple3<Long, Integer, Integer> input, Context context, Collector<Tuple2<Long, Integer>> out) throws Exception {
            if (input.f1 == 0){
                // if this is a FINISH job entry, register onTimer.
                context.timerService().registerEventTimeTimer(context.timestamp());
            }
            else if (input.f1 == 1){
                if (!mp.containsKey(input.f0) || (mp.containsKey(input.f0) && mp.get(input.f0) < input.f2)){
                    mp.put(input.f0, input.f2);
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, Integer>> out) throws Exception {
            // When job FINISH, output the maximum session length per job.
            out.collect(new Tuple2<>(ctx.getCurrentKey().getField(0), mp.get(ctx.getCurrentKey().getField(0))));
        }
    }
}
