package entities;

public class KeyedFeature extends Feature {

    public long key; // -1 represents no key

    public KeyedFeature(long key, double f1, double f2) {
        super(f1, f2);
        this.key = key;
    }

    @Override
    public String toString() {
        return String.format("entities.KeyedFeature{key=%d, f1=%f, f2=%f}", key, f1, f2);
    }
}