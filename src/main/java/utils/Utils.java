package utils;

import entities.Feature;
import entities.RawData;

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
                .process(new ToFeatureProcessingFunc());
        return output;
    }

    private static class ToFeatureProcessingFunc extends ProcessAllWindowFunction<RawData, Feature, TimeWindow> {
    private static final long serialVersionUID = -397143166557786027L;

    @Override
    public void process(Context context, Iterable<RawData> iterable, Collector<Feature> collector) throws Exception {
        List<RawData> ls = new ArrayList<>();
        iterable.forEach(ls::add);
        double p = Metrics.activePower(ls);
        double s = Metrics.apparentPower(ls);
        double q = Metrics.reactivePower(s,p);
        Feature point = new Feature(p,q);
        collector.collect(point);
    }
}
}
