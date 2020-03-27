package entities;

import org.apache.commons.math3.ml.clustering.Clusterable;

public class Feature implements Clusterable {

    public double f1;
    public double f2;

    /**
     * @param f1: Active power feature
     * @param f2: Reactive power feature
     */
    public Feature(double f1, double f2){
        this.f1 = f1;
        this.f2 = f2;
    }

    @Override
    public double[] getPoint() {
        double[] point = {f1, f2};
        return point;
    }

    @Override
    public String toString() {
        return String.format("entities.Point{f1=%f, f2=%f}", f1, f2);
    }
}
