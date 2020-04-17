package entities;

public class RawData {
    public long second;
    public long i;
    public double voltage;
    public double current;

    public RawData(long second, long i, double voltage, double current) {
        this.second = second;
        this.i = i;
        this.voltage = voltage;
        this.current = current;
    }

    @Override
    public String toString() {
        return "RawData{" +
                "second=" + second +
                ", i=" + i +
                ", voltage=" + voltage +
                ", current=" + current +
                '}';
    }
}
