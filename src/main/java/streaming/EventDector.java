package streaming;

import entities.ClusterStructure;
import entities.DetectedEvent;
import entities.KeyedFeature;
import entities.PredictedEvent;
import entities.Window2;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.security.Key;
import java.util.*;

public class EventDector {

    private double dbscanEps = 0.03;
    private int dbscanMinPoints = 2;
    private int lossThresh = 40;

    private Map<Integer, ClusterStructure> clusteringStructure;
    private Map<Integer, ClusterStructure> forwardClusteringStructure;
    private Map<Integer, ClusterStructure> backwardClusteringStructure;

    public EventDector() {
        this.dbscanEps = 0.05;
        this.dbscanMinPoints = 2;
    }

    public PredictedEvent
    predict(Window2 w2) {

        List<KeyedFeature> input = w2.getElements();

        // forward pass
        this.updateClustering(input);

        List<Tuple3<Integer, Integer, List<Integer>>> checkedClusters = this.checkEventModelConstraints();

        if (checkedClusters == null) return null; // add the next new_datapoint. We go back to step 1 of the forward pass.
        
        Tuple3<Integer, Integer, List<Integer>> eventClusterCombination = this.computeAndEvaluateLoss(checkedClusters);
        
        this.forwardClusteringStructure = this.clusteringStructure; // save the forward clustering structure

        if (eventClusterCombination == null) return null; // event not detected, go back to step 1 and add the next sample

        // if event detected, start backward pass
        this.backwardClusteringStructure = this.forwardClusteringStructure;
        Tuple3<Integer, Integer, List<Integer>> eventClusterCombinationBalanced = eventClusterCombination;
        
        for (int i = 1; i < input.size() - 1; i++) {
            List<KeyedFeature> inputCut = input.subList(i, input.size() - 1);

            this.updateClustering(inputCut);
            checkedClusters = this.checkEventModelConstraints();
            if (checkedClusters == null) {  // roleback with break
                eventClusterCombinationBalanced = this.rolebackBackwardPass("break", eventClusterCombinationBalanced, i, null);
                break;  // finished
            } else {    // compute the loss
                Tuple3<Integer, Integer, List<Integer>> eventClusterCombinationBelowLoss = this.computeAndEvaluateLoss(checkedClusters);
                if (eventClusterCombinationBelowLoss == null) { // roleback with break
                    eventClusterCombinationBalanced = this.rolebackBackwardPass("break", eventClusterCombinationBalanced, i, null);
                    break;  // finished
                } else {
                    eventClusterCombinationBalanced = this.rolebackBackwardPass("continue", eventClusterCombinationBalanced, i, eventClusterCombinationBelowLoss);
                }
            }
        }

        int eventStart = eventClusterCombinationBalanced.f2.get(0);
        int eventEnd = eventClusterCombinationBalanced.f2.get(eventClusterCombinationBalanced.f2.size() - 1);
        return new PredictedEvent(eventStart, eventEnd);
    }

    public Map<Integer, ClusterStructure> getClusteringStructure() {
        return clusteringStructure;
    }

    public void updateClustering(List<KeyedFeature> input) {
        // save point index in a map
        List<KeyedFeature> points = new ArrayList<>();
        points.addAll(input);

        Map<KeyedFeature, Integer> indexMap = new HashMap<KeyedFeature, Integer>();
        for (int i=0; i < points.size(); i++){
            indexMap.put(points.get(i), i);
        }

        DBSCANClusterer<KeyedFeature> dbscan = new DBSCANClusterer<>(this.dbscanEps, this.dbscanMinPoints);
        System.out.println(points);
        List<Cluster<KeyedFeature>> clusters = dbscan.cluster(points);
        Map<Integer, ClusterStructure> clusteringStructure = new HashMap<>();
        System.out.println(clusters);
        for(int cluster_i = 0; cluster_i < clusters.size(); cluster_i++){
            // calculate Loc
            List<KeyedFeature> ls = clusters.get(cluster_i).getPoints();
            System.out.println(cluster_i);
            System.out.println(ls);
            ClusterStructure clusterStructure = extractClusterStructure(ls, indexMap);
            points.removeAll(ls);
            clusteringStructure.put(cluster_i, clusterStructure);
        }
        if (points.size() > 0){
            clusteringStructure.put(-1, extractClusterStructure(points, indexMap));
        }
        this.clusteringStructure = clusteringStructure;
    }

    private ClusterStructure extractClusterStructure(List<KeyedFeature> ls, Map<KeyedFeature, Integer> indexMap){
        int u = indexMap.keySet().size()+1;
        int v = -1;
        List<Integer> idxLs = new ArrayList<>();
        for (KeyedFeature each : ls){
            int idx = indexMap.get(each);
            idxLs.add(idx);
            v = idx>v?idx:v;
            u = idx<u?idx:u;
        }
        double loc = ls.size()/(v-u+1);
        ClusterStructure clusterStructure = new ClusterStructure(idxLs, u, v, loc);
        return clusterStructure;
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

    private Tuple3<Integer, Integer, List<Integer>>
    rolebackBackwardPass(String status, 
                        Tuple3<Integer, Integer, List<Integer>> eventClusterCombinationBalanced,
                        int i,
                        Tuple3<Integer, Integer, List<Integer>> eventClusterCombinationBelowLoss) {
        // TO DO

        return null;
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