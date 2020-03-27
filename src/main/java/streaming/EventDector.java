package streaming;

import entities.ClusterStructure;
import entities.DetectedEvent;
import entities.KeyedFeature;
import entities.Window2;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import java.util.*;

public class EventDector {

    private double dbscanEps;
    private int dbscanMinPoints;
    private int lossThresh = 40;

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
        List<Integer> eventModelLossList = new ArrayList<>();
        int minLossIndex = -1;
        int minLoss = Integer.MAX_VALUE;
        for (int i = 0; i < checkedClusters.size(); i++) {
            Tuple3<Integer, Integer, List<Integer>> item = checkedClusters.get(i);
            Integer c1 = item.f0;
            Integer c2 = item.f1;
            List<Integer> eventIntervalIndices = item.f2;
            Integer lowerEventBound_U = eventIntervalIndices.get(0) - 1;    // the interval starts at u + 1
            Integer upperEventBound_V = eventIntervalIndices.get(eventIntervalIndices.size() - 1) + 1; // the interval ends at v -1
            List<Integer> c1Indices = this.clusteringStructure.get(c1).idxList;
            List<Integer> c2Indices = this.clusteringStructure.get(c2).idxList;
            
            Tuple3<Integer, Integer, Integer> errorsNums = computeErrorNumbers(c1Indices, c2Indices, lowerEventBound_U, upperEventBound_V);
            Integer eventModelLoss = errorsNums.f0 + errorsNums.f1 + errorsNums.f2; // a + b + c
            eventModelLossList.add(eventModelLoss);

            if (eventModelLoss < minLoss) {
                minLoss = eventModelLoss;
                minLossIndex = i;
            }
        }

        if (minLoss <= this.lossThresh)
            return checkedClusters.get(minLossIndex);

        return null;
    }

    private void rolebackBackwardPass() {
        // TO DO
    }

    private Tuple3<Integer, Integer, Integer> computeErrorNumbers(List<Integer> c1Indices, List<Integer> c2Indices, 
                                                                  Integer lowerThresh, Integer upperThresh) {
        Tuple3<Integer, Integer, Integer> counter = new Tuple3<>();
        
        // c2 <= u || u < c2 < v, c2 should >= v
        for (int i = 0; i < c2Indices.size(); i++) {
            if (c2Indices.get(i) <= lowerThresh) counter.f0++;
            else if (c2Indices.get(i) < upperThresh) counter.f2++;
        }

        // c1 >= v || u < c1 < v, c1 should <= u
        for (int i = 0; i < c1Indices.size(); i++) {
            if (c1Indices.get(i) >= upperThresh) counter.f1++;
            else if (c1Indices.get(i) > lowerThresh) counter.f2++;
        }

        return counter;
    }
}