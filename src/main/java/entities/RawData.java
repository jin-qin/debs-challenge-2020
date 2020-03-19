package entities;

public class RawData {
    public int i;
    public double voltage;
    public double current;

    public RawData(int i, double voltage, double current) {
        this.i = i;
        this.voltage = voltage;
        this.current = current;
    }

    @Override
    public String toString() {
        return "RawData{" +
                "i=" + i +
                ", voltage=" + voltage +
                ", current=" + current +
                '}';
    }
}
