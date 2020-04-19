package entities;

import org.apache.commons.math3.ml.clustering.Clusterable;

public class Feature implements Clusterable {

    public long idx;
    public double f1;
    public double f2;
    public long size;
    /**
     * @param f1: Active power feature
     * @param f2: Reactive power feature
     */

    public Feature(long idx, double f1, double f2, long size){
        this.idx = idx;
        this.f1 = f1;
        this.f2 = f2;
        this.size = size;
    }

    @Override
    public double[] getPoint() {
        double[] point = {f1, f2};
        return point;
    }

    @Override
    public String toString() {
        return "Feature{" +
                "idx=" + idx +
                ", f1=" + f1 +
                ", f2=" + f2 +
                ", size=" + size +
                '}';
    }
}
