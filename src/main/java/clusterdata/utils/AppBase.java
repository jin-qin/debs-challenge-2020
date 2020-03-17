package clusterdata.utils;

import clusterdata.datatypes.JobEvent;
import clusterdata.datatypes.TaskEvent;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class AppBase {
    public static SourceFunction<JobEvent> jobEvents = null;
    public static SourceFunction<TaskEvent> taskEvents = null;
    public static SourceFunction<String> strings = null;
    public static SinkFunction out = null;
    public static int parallelism = 4;

    public final static String pathToJobEventData = "/Users/runqitian/Workspace/Spring2020/CS591K1/assignment2/clusterdata-2011-2/job_events";
    public final static String pathToTaskEventData = "/Users/runqitian/Workspace/Spring2020/CS591K1/assignment2/clusterdata-2011-2/task_events";


    public static SourceFunction<JobEvent> jobSourceOrTest(SourceFunction<JobEvent> source) {
        if (jobEvents == null) {
            return source;
        }
        return jobEvents;
    }

    public static SourceFunction<TaskEvent> taskSourceOrTest(SourceFunction<TaskEvent> source) {
        if (taskEvents == null) {
            return source;
        }
        return taskEvents;
    }

    public static SinkFunction<?> sinkOrTest(SinkFunction<?> sink) {
        if (out == null) {
            return sink;
        }
        return out;
    }

    public static SourceFunction<String> stringSourceOrTest(SourceFunction<String> source) {
        if (strings == null) {
            return source;
        }
        return strings;
    }

    public static void printOrTest(org.apache.flink.streaming.api.datastream.DataStream<?> ds) {
        if (out == null) {
            ds.print();
        } else {
            ds.addSink(out);
        }
    }
}
