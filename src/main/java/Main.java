import entities.DetectedEvent;
import entities.Feature;
import entities.RawData;

import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import request.DataSource;
import streaming.Query1Streaming;
import utils.Utils;

public class Main {
    public static void main(String[] args) throws Exception{
        // set up query connection

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<RawData> input = env
                .addSource(new DataSource(1000))
                .setParallelism(1);

        DataStream<Feature> features = Utils.computeInputSignal(input);
        DataStream<DetectedEvent> result = Query1Streaming.start(features);
        result.print().setParallelism(1);

        env.execute("Number of busy machines every 5 minutes over the last 15 minutes");
    }

}
