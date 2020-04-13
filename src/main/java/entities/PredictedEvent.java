package entities;

public class PredictedEvent {
    public int eventStart;
    public int eventEnd;

    public PredictedEvent(int e_start, int e_end) {
        this.eventStart = e_start;
        this.eventEnd = e_end;
    }

    @Override
    public String toString() {
        return String.format("entities.PredictedEvent{eventStart=%d, eventEnd=%d}", eventStart, eventEnd);
    }
}