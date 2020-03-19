import entities.RawData;
import org.apache.commons.collections.iterators.IteratorEnumeration;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.Metrics;

import javax.xml.crypto.Data;
import java.util.*;

public class EventDector{

    private double dbscanEps;
    private int dbscanMinPoints;
    private int w2Size;
    private Map<Integer, ClusterStructure> clusteringStructure;

    public EventDector(){
        this.dbscanEps = 0.03;
        this.dbscanMinPoints = 2;
        this.w2Size = 100;
    }

    public DataStream<Point> computeInputSignal(DataStream<RawData> input){
        DataStream<Point> output = input.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .process(new ToFeatureProcessingFunc());
        return output;
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

    static class ToFeatureProcessingFunc
            extends ProcessAllWindowFunction<RawData, Point, TimeWindow> {

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

}
