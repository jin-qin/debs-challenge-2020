import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Collection;


public class Point implements Clusterable {

    private double f1;
    private double f2;

    /**
     * @param f1: Active power feature
     * @param f2: Reactive power feature
     */
    public Point(int index, double f1, double f2){
        this.f1 = f1;
        this.f2 = f2;
    }

    @Override
    public double[] getPoint() {
        double[] point = {f1, f2};
        return point;
    }
}
