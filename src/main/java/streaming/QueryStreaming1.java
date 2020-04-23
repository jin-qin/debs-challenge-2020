package streaming;

import entities.*;

import java.util.*;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import utils.Config;

public class QueryStreaming1 {
    public static DataStream<DetectedEvent> start(DataStream<Feature> features) {
        DataStream<DetectedEvent> result = features
                .process(new PredictFunc1());
        return result;
    }
}

class PredictFunc1 extends ProcessFunction<Feature, DetectedEvent> {
    private static final long serialVersionUID = -5973216181346355124L;

    private Window2 w2 = new Window2(true);
    private EventDetector1 ed = new EventDetector1();
    private Long windowStartIndex = -1L;
    private Long currentWindowStart = -1L;

    @Override
    public void processElement(Feature feature, Context ctx, Collector<DetectedEvent> out) throws Exception {
        if (w2.size() <= 0) {
            windowStartIndex = feature.idx;
            currentWindowStart = feature.idx;
        }
        w2.addElement(feature);
        
        PredictedEvent e = ed.predict(w2);

        if (e == null) {
            if (w2.size() > Config.w2_size) {
                w2.clear();
            }

            out.collect(new DetectedEvent(feature.idx, false, -1));
            return;
        }

        int meanEventIndex = (e.eventStart + e.eventEnd) / 2;

        List<Feature> subWindow = w2.subWindow(e.eventEnd, w2.size());
        w2.setW2(subWindow);

        windowStartIndex = windowStartIndex + e.eventEnd;
        out.collect(new DetectedEvent(feature.idx, true, currentWindowStart + meanEventIndex + 1));
        currentWindowStart = windowStartIndex;
    }
}