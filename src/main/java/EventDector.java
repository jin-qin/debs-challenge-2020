import org.apache.commons.collections.iterators.IteratorEnumeration;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

public class EventDector{

    private double dbscanEps;
    private int dbscanMinPoints;
    private Map<Integer, ClusterStructure> clusteringStructure;

    public EventDector(){

    }

    public void updateClustering(List<Point> inputs){

        // save point index in a map
        Map<Point, Integer> toIndex = new HashMap<Point, Integer>();
        for (int i=0; i<inputs.size(); i++){
            toIndex.put(inputs.get(i), i);
        }

        DBSCANClusterer<Point> dbscan = new DBSCANClusterer<>(this.dbscanEps, this.dbscanMinPoints);
        List<Cluster<Point>> clusters = dbscan.cluster(inputs);
        Map<Integer, ClusterStructure> clusteringStructure = new HashMap<>();
        for(int cluster_i =0; cluster_i < clusters.size(); cluster_i++){
            Cluster<Point> cluster = clusters.get(cluster_i);

            // calculate Loc
            List<Point> ls= cluster.getPoints();
            int u = inputs.size()+1;
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
        this.clusteringStructure = clusteringStructure;

    }

}
