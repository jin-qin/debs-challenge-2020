package entities;

import utils.Config;

public class KeyedFeature extends Feature {

    public long key;
    public long offset;

    // idx = key * partition_size + offset
    public KeyedFeature(long key, long offset,long idx, double f1, double f2) {
        super(idx, f1, f2);
        this.key = key;
        this.offset = offset;
    }

    @Override
    public String toString() {
        if (Config.debug){
            return "[" + key + ", "  + offset + "," + idx + ", " + f1 + ", " + f2 + "]";
        }
        return "KeyedFeature{" +
                "key=" + key +
                ", offset=" + offset +
                ", idx=" + idx +
                ", f1=" + f1 +
                ", f2=" + f2 +
                '}';
    }
}