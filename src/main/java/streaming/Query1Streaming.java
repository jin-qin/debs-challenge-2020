package streaming;

import entities.DetectedEvent;
import entities.Feature;
import entities.KeyedFeature;
import entities.PredictedEvent;
import entities.Window2;

import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import utils.Config;

public class Query1Streaming {
    public static void start(DataStream<Feature> features) {
        features.flatMap(new AddKeyMapper())
                .keyBy(e -> e.key)
                .map(new PredictMapper());
    }

    public static AddKeyMapper newAddKeyMapper() {
        return newAddKeyMapper();
    }
}

class AddKeyMapper implements FlatMapFunction<Feature, KeyedFeature> {
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

class PredictMapper implements MapFunction<KeyedFeature, DetectedEvent> {
    private static final long serialVersionUID = -5973216181346355124L;

    public Window2 w2 = new Window2();
    private EventDector ed = new EventDector();

    public int windowStartIndex = 1; // why?
    public int currentWindowStart = 1; // why?
    public int batchCounter = 0;

    @Override
    public DetectedEvent map(KeyedFeature feature) throws Exception {
        batchCounter++;

        w2.addElement(feature);
        PredictedEvent e = ed.predict(w2);

        if (e == null)
            return new DetectedEvent(batchCounter, false, -1);

        int meanEventIndex = (e.eventStart + e.eventEnd) / 2;

        List<KeyedFeature> subWindow = w2.subWindow(e.eventEnd, w2.size());
        w2.setW2(subWindow);

        windowStartIndex += e.eventEnd;
        currentWindowStart = windowStartIndex;

        return new DetectedEvent(batchCounter, true, currentWindowStart + meanEventIndex);
    }
}
