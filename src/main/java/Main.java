import entities.DetectedEvent;
import entities.Event;
import entities.Feature;
import entities.KeyedFeature;
import entities.PredictedEvent;
import entities.RawData;
import entities.Window2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import request.DataSource;
import streaming.Query1Streaming;
import utils.Config;
import utils.Utils;

import java.security.Key;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
    public static void main(String[] args) throws Exception{
        // set up query connection

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<RawData> input = env
                .addSource(new DataSource())
                .setParallelism(1);

        DataStream<Feature> features = Utils.computeInputSignal(input);
        DataStream<DetectedEvent> result = Query1Streaming.start(features);
        result.print();
        env.execute("Number of busy machines every 5 minutes over the last 15 minutes");

    }

}
