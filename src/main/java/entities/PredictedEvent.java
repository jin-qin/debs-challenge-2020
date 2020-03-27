package entities;

public class PredictedEvent {
    public long eventStart;
    public long eventEnd;

    PredictedEvent(long e_start, long e_end) {
        this.eventStart = e_start;
        this.eventEnd = e_end;
    }

    @Override
    public String toString() {
        return String.format("entities.PredictedEvent{eventStart=%d, eventEnd=%d}", eventStart, eventEnd);
    }
}