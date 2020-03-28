package entities;

public class DetectedEvent {
    private long windowStart;
    private boolean eventDetected;
    private long eventIndMean;

    public DetectedEvent(long s, boolean d, long event_s) {
        this.windowStart = s;
        this.eventDetected = d;
        this.eventIndMean = event_s;
    }

    public long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(long windowStart) {
        this.windowStart = windowStart;
    }

    public boolean isEventDetected() {
        return eventDetected;
    }

    public void setEventDetected(boolean eventDetected) {
        this.eventDetected = eventDetected;
    }

    public long getEventIndMean() {
        return eventIndMean;
    }

    public void setEventIndMean(long eventIndMean) {
        this.eventIndMean = eventIndMean;
    }

    @Override
    public String toString() {
        return String.format("DetectedEvent{windowStart:%d, eventDetected:%b, eventIndMean:%d}", windowStart, eventDetected, eventIndMean);
    }
}