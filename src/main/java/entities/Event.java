package entities;

public class Event {
    private long s;
    private long d;
    private long event_s;

    public Event(long s, long d, long event_s) {
        this.s = s;
        this.d = d;
        this.event_s = event_s;
    }

    @Override
    public String toString() {
        return "Event{" +
                "s=" + s +
                ", d=" + d +
                ", event_s=" + event_s +
                '}';
    }
}
