import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import entities.RawData;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import request.DataSource;
import request.Query1Client;
import utils.Utils;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Main {

    public static void main(String[] args) throws Exception{
        // set up query connection

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<RawData> inputStream = env
                .addSource(new DataSource())
                .setParallelism(1);
        inputStream.print();
//        DataStream<List<Integer>> output = inputStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(50))).process(new MyProcessWindowFunction());
//        output.print();
        env.execute("Number of busy machines every 5 minutes over the last 15 minutes");

    }

    static class MyProcessWindowFunction
            extends ProcessAllWindowFunction<RawData, List<Integer>, TimeWindow> {


        @Override
        public void process(Context context, Iterable<RawData> iterable, Collector<List<Integer>> collector) throws Exception {
            List<Integer> ls = new ArrayList<>();
            for (RawData each: iterable){
                System.out.println(each);
                ls.add(each.i);
            }
            Collections.sort(ls);
            collector.collect(ls);
        }
    }
}
