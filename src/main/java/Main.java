import entities.DetectedEvent;
import entities.Feature;
import entities.RawData;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import request.DataSourceForQuery1;
import request.DataSourceForQuery2;
import streaming.Query1Streaming;
import utils.Utils;

public class Main {
    public static void main(String[] args) throws Exception{
        // set up query connection
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // start the data generator
        env.setParallelism(1);

        DataStream<RawData> input = env
                .addSource(new DataSourceForQuery2())
                .setParallelism(1);

        DataStream<Feature> features = Utils.computeInputSignal(input);
        DataStream<DetectedEvent> result = Query1Streaming.start(features);
        result.print().setParallelism(1);

        env.execute("DEBS Challenge 2020");
    }

}
