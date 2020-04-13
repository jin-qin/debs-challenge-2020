package entities;

public class DetectedEvent {
    private long batchCounter;
    private boolean eventDetected;
    private long eventIndMean;

    public DetectedEvent(long s, boolean d, long event_s) {
        this.batchCounter = s;
        this.eventDetected = d;
        this.eventIndMean = event_s;
    }

    public long getBatchCounter() {
        return batchCounter;
    }

    public void setBatchCounter(long batchCounter) {
        this.batchCounter = batchCounter;
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
        return String.format("DetectedEvent{batchCounter:%d, eventDetected:%b, eventIndMean:%d}", batchCounter, eventDetected, eventIndMean);
    }

    public String toJson() {
        return String.format("{'s':%d, 'd':%b, 'event_s':%d}", batchCounter, eventDetected, eventIndMean);
    }
}