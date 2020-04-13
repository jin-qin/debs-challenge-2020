package utils;

import entities.Feature;
import entities.RawData;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.List;

public class Utils {
    public static List<RawData> parseJson(String str){
        List<RawData> ls = new ArrayList<>();
        try {
            JSONObject obj = (JSONObject) new JSONParser().parse(str);
            if (obj.get("records").toString().equals("")){
                return ls;
            }
            JSONArray records = (JSONArray) obj.get("records");
            for (Object each: records){
                JSONObject jobj = (JSONObject)each;
                ls.add(new RawData(Long.parseLong(jobj.get("i").toString()), Double.parseDouble(jobj.get("voltage").toString()), Double.parseDouble(jobj.get("current").toString())));
            }
            return ls;
        }catch (Exception e){
            e.printStackTrace();
        }
        return ls;
    }

    public static DataStream<Feature>
    computeInputSignal(DataStream<RawData> input) {
        DataStream<Feature> output = input.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .process(new ToFeatureProcessingFunc(1000));
        return output;
    }

    private static class ToFeatureProcessingFunc extends ProcessAllWindowFunction<RawData, Feature, TimeWindow> {
        private int windowSize;
        private static final long serialVersionUID = -397143166557786027L;

        public ToFeatureProcessingFunc(int windowSize){
            super();
            this.windowSize = windowSize;
        }

        @Override
        public void process(Context context, Iterable<RawData> iterable, Collector<Feature> collector) throws Exception {
            List<RawData> ls = new ArrayList<>();
            iterable.forEach(ls::add);
            long minIdx = Long.MAX_VALUE;
            for (RawData each: ls){
                if (each.i < minIdx){
                    minIdx = each.i;
                }
            }
            minIdx = minIdx / this.windowSize;
            double p = Metrics.activePower(ls);
            double s = Metrics.apparentPower(ls);
            double q = Metrics.reactivePower(s,p);
            
            if (Config.use_log) {
                p = Math.log(p);
                q = Math.log(q);
            }
            Feature point = new Feature(minIdx, p, q);
            collector.collect(point);
        }
    }

    public static List<Tuple2<Integer, Integer>> combination(List<Integer> list, int pair){
        List<Tuple2<Integer, Integer>> combination = new ArrayList<>();
        for (int i = 0; i < list.size()-1; i++){
            for (int j = i+1; j < list.size(); j++){
                combination.add(new Tuple2<Integer, Integer>(list.get(i), list.get(j)));
            }
        }
        return combination;
    }
}
