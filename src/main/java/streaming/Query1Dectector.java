package streaming;

import entities.*;
import utils.Config;

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

    public DetectedEvent dectedEvent(){
        for (KeyedFeature feature: features){
            if (windowStartIndex== -1) windowStartIndex = feature.offset;
            if (currentWindowStart == -1) currentWindowStart = feature.offset;

            w2.addElement(feature);

            PredictedEvent e = ed.predict(w2);

            if (e == null) {
                if (w2.size() > Config.w2_size) {
                    w2.removeFirst();

                    windowStartIndex += 1;
                    currentWindowStart += 1;
                }
                continue;
            }

            int meanEventIndex = (e.eventStart + e.eventEnd) / 2;
//            System.out.println(w2.getElements().get(e.eventStart));
//            System.out.println(w2.getElements().get(e.eventEnd));
            List<KeyedFeature> subWindow = w2.subWindow(e.eventEnd, w2.size());
            w2.setW2(subWindow);

            windowStartIndex = windowStartIndex + e.eventEnd;
            Long globalCurrentWindowStart = feature.key * Config.partion_size + currentWindowStart;
            currentWindowStart = windowStartIndex;

            return new DetectedEvent(feature.idx, true, globalCurrentWindowStart + meanEventIndex + 1);
        }
        return null;
    }

}

