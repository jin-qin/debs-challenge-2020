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
import utils.Utils;

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
        features.flatMap(new AddKeyMapper())
                .keyBy(e -> e.key)
                .flatMap(new PredictMapper());
        
        env.execute("Number of busy machines every 5 minutes over the last 15 minutes");

    }

    private static class AddKeyMapper implements FlatMapFunction<Feature, KeyedFeature> {
        private static final long serialVersionUID = 192888106183989331L;

        @Override
        public void flatMap(Feature value, Collector<KeyedFeature> out) throws Exception {

        }
    }

    private static class PredictMapper implements FlatMapFunction<KeyedFeature, PredictedEvent> {
        private static final long serialVersionUID = -5973216181346355124L;

        private static Window2 w2 = new Window2();
        private static EventDector ed = new EventDector();

        @Override
        public void flatMap(KeyedFeature value, Collector<PredictedEvent> out) throws Exception {
            w2.addElement(value);
            ed.predict(w2);
        }
    }

}
