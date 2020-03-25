package entities;

public class RawData {
    public long i;
    public double voltage;
    public double current;

    public RawData(long i, double voltage, double current) {
        this.i = i;
        this.voltage = voltage;
        this.current = current;
    }

    @Override
    public String toString() {
        return String.format("RawData{i=%d, voltage=%f, current=%f}", i, voltage, current);
    }
}
