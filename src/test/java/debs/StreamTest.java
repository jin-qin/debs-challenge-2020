package debs;

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
    public void keyedFeatureTest() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<RawData> input = env
                .addSource(new DataSource(120))
                .setParallelism(1);

        DataStream<Feature> features = Utils.computeInputSignal(input);
        DataStream<KeyedFeature> output = features.flatMap(Query1Streaming.newAddKeyMapper());
        output.print();
        env.execute("query1 running");
    }
}
