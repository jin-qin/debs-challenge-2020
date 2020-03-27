package entities;

public class KeyedFeature extends Feature {

    public long key;
    public long offset;


    public KeyedFeature(long key, long offset,long idx, double f1, double f2) {
        super(idx, f1, f2);
        this.key = key;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "KeyedFeature{" +
                "key=" + key +
                ", offset=" + offset +
                ", idx=" + idx +
                ", f1=" + f1 +
                ", f2=" + f2 +
                '}';
    }
}