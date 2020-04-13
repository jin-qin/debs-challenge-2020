package streaming;

import entities.ClusterStructure;
import entities.KeyedFeature;
import entities.PredictedEvent;
import entities.Window2;

import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import utils.*;
import java.util.*;

public class EventDetector {

    private double dbscanEps = 0.03;
    private int dbscanMinPoints = 1;
    private int lossThresh = 40;
    private double temp_eps = 0.8;
    private boolean debug = false;

    private Map<Integer, ClusterStructure> clusteringStructure;
    private Map<Integer, ClusterStructure> forwardClusteringStructure;
    private Map<Integer, ClusterStructure> backwardClusteringStructure;

    public EventDetector() {
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
            List<KeyedFeature> inputCut = input.subList(i, input.size());

            this.updateClustering(inputCut);
            checkedClusters = this.checkEventModelConstraints();
            if (checkedClusters == null) {  // roleback with break
                eventClusterCombinationBalanced = this.rollbackBackwardPass("break", eventClusterCombinationBalanced, i, null);
                break;  // finished
            } else {    // compute the loss
                Tuple3<Integer, Integer, List<Integer>> eventClusterCombinationBelowLoss = this.computeAndEvaluateLoss(checkedClusters);
                if (eventClusterCombinationBelowLoss == null) { // roleback with break
                    eventClusterCombinationBalanced = this.rollbackBackwardPass("break", eventClusterCombinationBalanced, i, null);
                    break;  // finished
                } else {
                    eventClusterCombinationBalanced = this.rollbackBackwardPass("continue", eventClusterCombinationBalanced, i, eventClusterCombinationBelowLoss);
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
        // System.out.println(points);
        List<Cluster<KeyedFeature>> clusters = dbscan.cluster(points);
        Map<Integer, ClusterStructure> clusteringStructure = new HashMap<>();
        // System.out.println(clusters);
        for(int cluster_i = 0; cluster_i < clusters.size(); cluster_i++){
            // calculate Loc
            List<KeyedFeature> ls = clusters.get(cluster_i).getPoints();
            // System.out.println(cluster_i);
            // System.out.println(ls);
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
        double loc = ls.size()/(v-u+1.0);
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

    private List<Tuple3<Integer, Integer, List<Integer>>> checkEventModelConstraints() {

        int n_non_noise_clusters =  this.clusteringStructure.containsKey(-1) ? this.clusteringStructure.size() - 1 : this.clusteringStructure.size();

        if (this.debug) System.out.println("Number of non noise_clusters: "+ n_non_noise_clusters);

        if (n_non_noise_clusters < 2) return null; //check (1) not passed

        Map<Integer, ClusterStructure> check_two_clustering = new HashMap<>(); //store the clusters that pass the test in a new structure

        for (Map.Entry<Integer, ClusterStructure> cluster_i_structure : this.clusteringStructure.entrySet()){
            if (cluster_i_structure.getKey() != -1) { // for all non noise clusters

                if (this.debug) System.out.println(cluster_i_structure.getKey() + "   | " + cluster_i_structure.getValue().loc);

                if (cluster_i_structure.getValue().loc >= 1.0 - this.temp_eps) { //the central condition of condition (2)
                    check_two_clustering.put(cluster_i_structure.getKey(), cluster_i_structure.getValue());
                }
            }
        }

        if (this.debug) System.out.println("Number of clusters that pass temporal locality epsilon(Check 2): " + n_non_noise_clusters + " (min 2 clusters) ");

        if (check_two_clustering.size() < 2) return null; //check (2) not passed

        /*
        # Two clusters C1 and C2 do not interleave in the time domain.
        # There is a point s in C1 for which all points n > s do not belong to C1 anymore.
        # There is also a point i in C2 for which all points n < i do not belong to C2 anymore.
        # i.e. the maximum index s of C1 has to be smaller then the minimum index of C2
        */
        List<Tuple3<Integer, Integer, List<Integer>>> checked_clusters = new ArrayList<>();

        //We need to compare all pairs of clusters in order to find all pairs that fullfill condition (3)

        List<Integer> keylist = new ArrayList<>();
        for(Map.Entry<Integer, ClusterStructure> entry : check_two_clustering.entrySet()){ keylist.add(entry.getKey()); }

        List<Tuple2<Integer, Integer>> combinations = Utils.combination(keylist, 2);

        int c1, c2;
        // the cluster with the smaller u, occurs first in time, we name it C1 according to the paper terminology here
        for (Tuple2<Integer, Integer> pair : combinations){
            if(check_two_clustering.get(pair.f0).u < check_two_clustering.get(pair.f1).u){
                c1 = pair.f0;
                c2 = pair.f1;
            }else {
                c1 = pair.f1;
                c2 = pair.f0;
            }

            /*
            now we check if they are overlapping
            the maximum index of C1 has to be smaller then the minimum index of C2, then no point of C2 is in C1
            and the other way round, i.e all points in C1 have to be smaller then u of C2
            */
            List<Integer> c0_indices = new ArrayList<>();

            if (check_two_clustering.get(c1).v < check_two_clustering.get(c2).u) {// no overlap detected
                if (this.clusteringStructure.containsKey(-1)) {
                    c0_indices = this.clusteringStructure.get(-1).idxList;
                } else {
                    return null;
                }

                if (this.debug) {
                    System.out.println("No overlap between cluster " + c1 + " and " + c2 );
                    System.out.println("Potential event window (noise cluster indices:"  + c0_indices);
                    System.out.println("Cluster 1 v: " + check_two_clustering.get(c1).v);
                    System.out.println("Cluster 2 u: " + check_two_clustering.get(c2).u);

                }

                List<Integer> event_interval_t = new ArrayList<>();
                //check the condition, no overlap between the noise and steady state clusters allowed: u is lower, v upper bound
                for (int idx : c0_indices) {
                    if ((idx > check_two_clustering.get(c1).v) && (idx < check_two_clustering.get(c2).u)){
                        event_interval_t.add(idx);
                    }
                }

                if(this.debug) System.out.println("Event Interval between " + c1 + " and " + c2 + " with indices " + event_interval_t);

                /*  If the event_interval_t contains no points, we do not add it to the list too,
                i.e. this combinations does not contain a distinct event interval between the two steady state
                cluster sections.
                */
                if (event_interval_t.size() != 0) {
                    checked_clusters.add(new Tuple3<>(c1, c2, event_interval_t));
                }
            }
        }

        if(this.debug) System.out.println("Passed cluster-pairs: " + checked_clusters.size());

        if (checked_clusters.size() < 1) return null;

        return checked_clusters;
    }


    /**
     * @param checkedClusters (stream): of triples (c1, c2, event_interval_t)
     * @return eventClusterCombination (stream): the winning of triples (c1, c2, event_interval_t)
     */
    public Tuple3<Integer, Integer, List<Integer>>
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

    public Tuple3<Integer, Integer, List<Integer>>
    rollbackBackwardPass(String status,
                        Tuple3<Integer, Integer, List<Integer>> eventClusterCombinationBalanced,
                        int i,
                        Tuple3<Integer, Integer, List<Integer>> eventClusterCombinationBelowLoss) {
        // TO DO
        if (status.equals("break")){
            List<Integer> ls = new ArrayList<>();
            for (Integer each: eventClusterCombinationBalanced.f2){
                ls.add(each + i);
            }
            eventClusterCombinationBalanced.setField(ls, 2);
            for (Map.Entry<Integer, ClusterStructure> each: this.backwardClusteringStructure.entrySet()){
                int cluster_i = each.getKey();
                ClusterStructure cluster_i_structure = each.getValue();
                ls = new ArrayList<>();
                for (Integer idx: cluster_i_structure.idxList){
                    ls.add(idx + i - 1);
                }
                cluster_i_structure.idxList = ls;
                cluster_i_structure.u = cluster_i_structure.u + i - 1;
                cluster_i_structure.v = cluster_i_structure.v + i - 1;
            }
            return eventClusterCombinationBalanced;
        }
        else if(status.equals("continue")){
            this.backwardClusteringStructure = clusteringStructure;
            eventClusterCombinationBalanced = eventClusterCombinationBelowLoss;
            return eventClusterCombinationBalanced;
        }
        else{
            ;
        }
        return null;
    }

    private Tuple3<Integer, Integer, Integer> computeErrorNumbers(List<Integer> c1Indices, List<Integer> c2Indices,
                                                                  Integer lowerThresh, Integer upperThresh) {
        Tuple3<Integer, Integer, Integer> counter = new Tuple3<>(0, 0, 0);

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
