package entities;

public class DetectedEvent {
    private long eventStart;
    private long eventEnd;

    public DetectedEvent(long s, long e) {
        this.eventStart = s;
        this.eventEnd = e;
    }

    @Override
    public String toString() {
        return String.format("DetectedEvent{eventStart:%d, eventEnd:%d}", eventStart, eventEnd);
    }
}