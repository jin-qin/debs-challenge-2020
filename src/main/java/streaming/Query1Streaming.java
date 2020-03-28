package streaming;

import entities.DetectedEvent;
import entities.Feature;
import entities.KeyedFeature;
import entities.PredictedEvent;
import entities.Window2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;
import utils.Config;

public class Query1Streaming {
    public static class AddKeyMapper implements FlatMapFunction<Feature, KeyedFeature> {
        private static final long serialVersionUID = 192888106183989331L;

        @Override
        public void flatMap(Feature value, Collector<KeyedFeature> out) throws Exception {
            long offset = value.idx % Config.partion_size;
            long partionIdx = value.idx / Config.partion_size; // return the floor long value
            KeyedFeature keyed = new KeyedFeature(partionIdx, offset, value.idx, value.f1, value.f2);
            out.collect(keyed);
            if (offset < Config.w2_size && partionIdx!=0){
                KeyedFeature keyedAdd = new KeyedFeature(partionIdx-1, offset, value.idx, value.f1, value.f2);
                out.collect(keyedAdd);
            }
        }
    }

    public static class PredictMapper implements FlatMapFunction<KeyedFeature, PredictedEvent> {
        private static final long serialVersionUID = -5973216181346355124L;

        private static Window2 w2 = new Window2();
        private static EventDector ed = new EventDector();

        private static int windowStartIndex = 0;

        @Override
        public void flatMap(KeyedFeature value, Collector<PredictedEvent> out) throws Exception {
            w2.addElement(value);
            PredictedEvent e = ed.predict(w2);
            if (e != null) out.collect(e);
        }
    }

    public static class DetectedEventMapper implements MapFunction<PredictedEvent, DetectedEvent> {
        private static final long serialVersionUID = 8598912969202017900L;

        @Override
        public DetectedEvent map(PredictedEvent value) throws Exception {
            return null;
        }
    }
}
