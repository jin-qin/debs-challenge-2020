package debs;

import entities.DetectedEvent;
import entities.Feature;
import entities.KeyedFeature;
import entities.RawData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;
import request.DataSource;
import streaming.Query1Streaming;
import utils.Utils;

public class StreamTest {

    @Test
    public void keyedFeatureTest() throws Exception {
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

    @Test
    public void computeAndEvaluateLossTest() throws Exception {

    }

    @Test
    public void predictTest() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // env.setParallelism(2);

        // start the data generator
        DataStream<RawData> input = env
                .addSource(new DataSource(500))
                .setParallelism(1);

        DataStream<Feature> features = Utils.computeInputSignal(input);
        DataStream<DetectedEvent> result = Query1Streaming.start(features);
//        result.print();
        env.execute("Number of busy machines every 5 minutes over the last 15 minutes");
    }

}
