package entities;

public class DetectedEvent {
    private int windowStart;
    private boolean eventDetected;
    private int eventIndMean;

    public DetectedEvent(int s, boolean d, int event_s) {
        this.windowStart = s;
        this.eventDetected = d;
        this.eventIndMean = event_s;
    }

    @Override
    public String toString() {
        return String.format("DetectedEvent{windowStart:%d, eventDetected:%b, eventIndMean:%d}", windowStart, eventDetected, eventIndMean);
    }
}