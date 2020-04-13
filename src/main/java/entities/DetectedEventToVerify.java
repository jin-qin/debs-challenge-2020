package entities;

import java.util.List;

public class DetectedEventToVerify extends DetectedEvent{

    private KeyedFeature feature;
    private long eventStart;
    private long eventEnd;

    public DetectedEventToVerify(long s, boolean d, long event_s) {
        super(s, d, event_s);
    }
    public DetectedEventToVerify(long s, boolean d, long event_s, KeyedFeature feature, long eventStart, long eventEnd) {
        super(s, d, event_s);
        this.feature = feature;
        this.eventStart = eventStart;
        this.eventEnd = eventEnd;
    }

    public KeyedFeature getFeature() {
        return feature;
    }

    public void setFeature(KeyedFeature feature) {
        this.feature = feature;
    }

    public long getEventStart() {
        return eventStart;
    }

    public void setEventStart(long eventStart) {
        this.eventStart = eventStart;
    }

    public long getEventEnd() {
        return eventEnd;
    }

    public void setEventEnd(long eventEnd) {
        this.eventEnd = eventEnd;
    }
}
