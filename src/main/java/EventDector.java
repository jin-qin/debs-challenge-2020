import entities.DetectedEvent;
import entities.Event;
import entities.Point;
import entities.RawData;
import entities.Window2;
import scala.collection.parallel.ParIterableLike.FlatMap;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.Metrics;

import java.util.*;

public class EventDector {

    private double dbscanEps;
    private int dbscanMinPoints;
    private List<Point> w2; // window 2, to accpet each new pair of features (active and reactive power)
    private final int W2_SIZE = 100;
    // private Map<Integer, ClusterStructure> clusteringStructure;

    public EventDector() {
        this.dbscanEps = 0.03;
        this.dbscanMinPoints = 2;
    }

    public DataStream<Point>
    computeInputSignal(DataStream<RawData> input) {
        DataStream<Point> output = input.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .process(new ToFeatureProcessingFunc());
        return output;
    }

    public DataStream<DetectedEvent>
    predict(DataStream<Point> input) {
        DataStream<List<Point>> w2Stream = input.map((MapFunction<Point, List<Point>>) e -> {
            if (this.w2.size() > this.W2_SIZE) {
                this.w2.remove(0);
            }

            this.w2.add(e);
            return this.w2;
        });

        DataStream<Map<Integer, ClusterStructure>> clusteringStructureStream = this.updateClustering(w2Stream);

        DataStream<List<Tuple3<Integer, Integer, List<Integer>>>> checkedClustersStream = this.checkEventModelConstraints(clusteringStructureStream);
        
        DataStream<Tuple3<Integer, Integer, List<Integer>>> eventClusterCombinationStream = this.computeAndEvaluateLoss(checkedClustersStream, clusteringStructureStream);

        return null;
    }

    private DataStream<Map<Integer, ClusterStructure>>
    updateClustering(DataStream<List<Point>> inputs) {
        return inputs.map((MapFunction<List<Point>, Map<Integer, ClusterStructure>>) e -> {
            // save point index in a map
            Map<Point, Integer> toIndex = new HashMap<Point, Integer>();
            for (int i=0; i < e.size(); i++){
                toIndex.put(e.get(i), i);
            }

            DBSCANClusterer<Point> dbscan = new DBSCANClusterer<>(this.dbscanEps, this.dbscanMinPoints);
            List<Cluster<Point>> clusters = dbscan.cluster(e);
            Map<Integer, ClusterStructure> clusteringStructure = new HashMap<>();
            for(int cluster_i =0; cluster_i < clusters.size(); cluster_i++){
                Cluster<Point> cluster = clusters.get(cluster_i);

                // calculate Loc
                List<Point> ls= cluster.getPoints();
                int u = e.size()+1;
                int v = -1;
                List<Integer> idxLs = new ArrayList<>();
                for (Point each : ls){
                    int idx = toIndex.get(each);
                    idxLs.add(idx);
                    v = idx>v?idx:v;
                    u = idx<u?idx:u;
                }
                double loc = ls.size()/(v-u+1);
                ClusterStructure clusterStructure = new ClusterStructure(idxLs, u, v, loc);
                clusteringStructure.put(cluster_i, clusterStructure);
            }
            return clusteringStructure;
        });
    }

    /**
     * 
     * @return Tuple3<c1, c2, event_interval_t>
     * with c1 being the identifier of the first cluster, c2 the second cluster
     * in the c1 - c2 cluster-combination, that have passed the model 
     * checks. The event_interval_t are the indices of the datapoints in between the two clusters.
     */
    private DataStream<List<Tuple3<Integer, Integer, List<Integer>>>>
    checkEventModelConstraints(DataStream<Map<Integer, ClusterStructure>> clusteringStructureStream) {
        // It's better to use FlatMap here.
        return null;
    }

    /**
     * @param checkedClusters (stream): of triples (c1, c2, event_interval_t)
     * @return eventClusterCombination (stream): the winning of triples (c1, c2, event_interval_t)
     */
    private DataStream<Tuple3<Integer, Integer, List<Integer>>> 
    computeAndEvaluateLoss(DataStream<List<Tuple3<Integer, Integer, List<Integer>>>> checkedClustersStream,
                           DataStream<Map<Integer, ClusterStructure>> clusteringStructureStream) {
        return checkedClustersStream
                    .connect(clusteringStructureStream)
                    .flatMap(new computeLossFlatMapper());
    }

    private void rolebackBackwardPass() {
        // TO DO
    }

    private class computeLossFlatMapper 
    implements CoFlatMapFunction<List<Tuple3<Integer, Integer, List<Integer>>>, 
                                 Map<Integer, ClusterStructure>, Tuple3<Integer, 
                                 Integer, List<Integer>>> {
        private static final long serialVersionUID = 2653570300428136437L;

        @Override
        public void 
        flatMap1(List<Tuple3<Integer, Integer, List<Integer>>> checkedClusters, 
                Collector<Tuple3<Integer, Integer, List<Integer>>> out) throws Exception {

        }

        @Override
        public void 
        flatMap2(Map<Integer, ClusterStructure> clusteringStructure, 
                Collector<Tuple3<Integer, Integer, List<Integer>>> out) throws Exception {

        }
    }
}


class ToFeatureProcessingFunc
        extends ProcessAllWindowFunction<RawData, Point, TimeWindow> {
    private static final long serialVersionUID = -397143166557786027L;

    @Override
    public void process(Context context, Iterable<RawData> iterable, Collector<Point> collector) throws Exception {
        List<RawData> ls = new ArrayList<>();
        iterable.forEach(ls::add);
        double p = Metrics.activePower(ls);
        double s = Metrics.apparentPower(ls);
        double q = Metrics.reactivePower(s,p);
        Point point = new Point(p,q);
        collector.collect(point);
    }
}