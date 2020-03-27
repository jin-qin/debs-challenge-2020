import entities.DetectedEvent;
import entities.Event;
import entities.Feature;
import entities.KeyedFeature;
import entities.KeyedFeature;
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

    private Map<Integer, ClusterStructure> clusteringStructure;

    public EventDector() {
        this.dbscanEps = 0.03;
        this.dbscanMinPoints = 2;
    }

    public DataStream<DetectedEvent>
    predict(Window2 w2) {
        List<KeyedFeature> input = w2.getElements();

        // forward pass
        this.updateClustering(input);

        List<Tuple3<Integer, Integer, List<Integer>>> checkedClusters = this.checkEventModelConstraints();
        
        Tuple3<Integer, Integer, List<Integer>> eventClusterCombination = this.computeAndEvaluateLoss(checkedClusters);

        // if event detected, start backward pass
        return null;
    }

    private void updateClustering(List<KeyedFeature> inputs) {
        // save point index in a map
        Map<KeyedFeature, Integer> toIndex = new HashMap<KeyedFeature, Integer>();
        for (int i=0; i < inputs.size(); i++){
            toIndex.put(inputs.get(i), i);
        }

        DBSCANClusterer<KeyedFeature> dbscan = new DBSCANClusterer<>(this.dbscanEps, this.dbscanMinPoints);
        List<Cluster<KeyedFeature>> clusters = dbscan.cluster(inputs);
        Map<Integer, ClusterStructure> clusteringStructure = new HashMap<>();
        for(int cluster_i = 0; cluster_i < clusters.size(); cluster_i++){
            Cluster<KeyedFeature> cluster = clusters.get(cluster_i);

            // calculate Loc
            List<KeyedFeature> ls= cluster.getPoints();
            int u = inputs.size()+1;
            int v = -1;
            List<Integer> idxLs = new ArrayList<>();
            for (KeyedFeature each : ls){
                int idx = toIndex.get(each);
                idxLs.add(idx);
                v = idx>v?idx:v;
                u = idx<u?idx:u;
            }
            double loc = ls.size()/(v-u+1);
            ClusterStructure clusterStructure = new ClusterStructure(idxLs, u, v, loc);
            clusteringStructure.put(cluster_i, clusterStructure);
        }
        this.clusteringStructure = clusteringStructure;
    }

    /**
     * 
     * @return Tuple3<c1, c2, event_interval_t>
     * with c1 being the identifier of the first cluster, c2 the second cluster
     * in the c1 - c2 cluster-combination, that have passed the model 
     * checks. The event_interval_t are the indices of the datapoints in between the two clusters.
     */
    private List<Tuple3<Integer, Integer, List<Integer>>>
    checkEventModelConstraints() {
        return null;
    }

    /**
     * @param checkedClusters (stream): of triples (c1, c2, event_interval_t)
     * @return eventClusterCombination (stream): the winning of triples (c1, c2, event_interval_t)
     */
    private Tuple3<Integer, Integer, List<Integer>>
    computeAndEvaluateLoss(List<Tuple3<Integer, Integer, List<Integer>>> checkedClusters) {
        return null;
    }

    private void rolebackBackwardPass() {
        // TO DO
    }
}