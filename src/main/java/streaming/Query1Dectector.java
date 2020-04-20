package streaming;

import entities.*;
import org.apache.flink.api.java.tuple.Tuple2;
import utils.Config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Query1Dectector {

    private List<KeyedFeature> features;

    private Window2 w2 = new Window2();
    private EventDetector ed = new EventDetector();
    private long windowStartIndex = -1;
    private long currentWindowStart = -1;
    private Map<Long, KeyedFeature> mapTsFeature = new HashMap<>();

    public Query1Dectector(List<KeyedFeature> features){
        this.features = features;
    }

    public List<Tuple2<DetectedEvent, Long>> dectedEvent2(){
        List<Tuple2<DetectedEvent, Long>> outputBuffer = new ArrayList<>();
        for (KeyedFeature feature: features){
            if (windowStartIndex== -1) windowStartIndex = feature.offset;
            if (currentWindowStart == -1) currentWindowStart = feature.offset;

            w2.addElement(feature);

            PredictedEvent e = ed.predict(w2);
            if (e == null) {
                if (w2.size() > Config.w2_size) {
                    w2.clear();
                    currentWindowStart = -1;
                    windowStartIndex = -1;
                }
                continue;
            }

            int meanEventIndex = (e.eventStart + e.eventEnd) / 2;
            List<KeyedFeature> subWindow = w2.subWindow(e.eventEnd, w2.size());
            w2.setW2(subWindow);

            windowStartIndex = windowStartIndex + e.eventEnd;
            Long globalCurrentWindowStart = feature.key * Config.partion_size + currentWindowStart;
            currentWindowStart = windowStartIndex;

            outputBuffer.add(new Tuple2(new DetectedEvent(feature.idx, true, globalCurrentWindowStart + meanEventIndex + 1), globalCurrentWindowStart + e.eventEnd));
        }
        return outputBuffer;
    }

    public Tuple2<DetectedEvent, Long> dectedEvent(){
        for (KeyedFeature feature: features){
            if (windowStartIndex== -1) windowStartIndex = feature.offset;
            if (currentWindowStart == -1) currentWindowStart = feature.offset;

            w2.addElement(feature);

            PredictedEvent e = ed.predict(w2);
            if (e == null) {
                if (w2.size() > Config.w2_size) {
                    w2.clear();
                    currentWindowStart = -1;
                    windowStartIndex = -1;
                }
                continue;
            }

            int meanEventIndex = (e.eventStart + e.eventEnd) / 2;
            List<KeyedFeature> subWindow = w2.subWindow(e.eventEnd, w2.size());
            w2.setW2(subWindow);

            windowStartIndex = windowStartIndex + e.eventEnd;
            Long globalCurrentWindowStart = feature.key * Config.partion_size + currentWindowStart;
            currentWindowStart = windowStartIndex;

            return new Tuple2(new DetectedEvent(feature.idx, true, globalCurrentWindowStart + meanEventIndex + 1), globalCurrentWindowStart + e.eventEnd);
        }
        return null;
    }

}

