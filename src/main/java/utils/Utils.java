package utils;

import entities.Feature;
import entities.RawData;
import request.QueryClient;

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

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {

    public static List<RawData> parseJson(String str, long currentTime) {
        List<RawData> ls = new ArrayList<>();
        try {
            JSONObject obj = (JSONObject) new JSONParser().parse(str);
            if (obj.get("records").toString().equals("")) {
                return ls;
            }
            JSONArray records = (JSONArray) obj.get("records");
            for (Object each : records) {
                JSONObject jobj = (JSONObject) each;
                ls.add(new RawData(currentTime, Long.parseLong(jobj.get("i").toString()),
                        Double.parseDouble(jobj.get("voltage").toString()),
                        Double.parseDouble(jobj.get("current").toString())));
            }
            return ls;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ls;
    }

    public static DataStream<Feature> computeInputSignal(DataStream<RawData> input) {
        DataStream<Feature> output = input.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(Config.w1_size)))
                .process(new ToFeatureProcessingFunc());
        return output;
    }

    private static class ToFeatureProcessingFunc extends ProcessAllWindowFunction<RawData, Feature, TimeWindow> {
        private static final long serialVersionUID = -397143166557786027L;

        public ToFeatureProcessingFunc() {
            super();
        }

        private Map<Long, RawData> prevState = null;
        private long counter = 0;

        @Override
        public void process(Context context, Iterable<RawData> iterable, Collector<Feature> collector)
                throws Exception {
            List<RawData> ls = new ArrayList<>();
            iterable.forEach(ls::add);
            // System.out.println("computeSignal" + (ls.get(0).second) + " " + ls.size());
//            if (ls.get(0).second==800){
//                System.out.println("breakpoint ComputeSignal");
//            }

            if (prevState == null) {
                prevState = new HashMap<>();
                for (RawData each : ls) {
                    prevState.put(each.i % Config.w1_size, each);
                }
            } else {
                for (RawData each : ls) {
                    prevState.put(each.i % Config.w1_size, each);
                }
                if (ls.size() < Config.w1_size) {
                    ls.clear();
                    ls.addAll(prevState.values());
                }
            }

            double p = Metrics.activePower(ls);
            double s = Metrics.apparentPower(ls);
            double q = Metrics.reactivePower(s, p);

            if (Config.use_log) {
                p = Math.log(p);
                q = Math.log(q);
            }
            Feature point = new Feature(counter, p, q, ls.size());
            collector.collect(point);
            counter++;
        }
    }

    public static List<Tuple2<Integer, Integer>> combination(List<Integer> list, int pair) {
        List<Tuple2<Integer, Integer>> combination = new ArrayList<>();
        for (int i = 0; i < list.size() - 1; i++) {
            for (int j = i + 1; j < list.size(); j++) {
                combination.add(new Tuple2<Integer, Integer>(list.get(i), list.get(j)));
            }
        }
        return combination;
    }

    public static void waitForBenchmarkSystem(String host, int port) throws InterruptedException {
        long startTime = System.currentTimeMillis();

        System.out.println("Waiting for the benchmark system becoming available...");
        while (System.currentTimeMillis() < startTime + Config.max_wait_benchmark_seconds * 1000) {
            String serverIP = System.getenv("BENCHMARK_SYSTEM_URL");
            QueryClient qc = new QueryClient(serverIP, "");
            if (qc.checkConnection()) {
                System.out.println("Connected to the benchmark system");
                return;
            }
        }

        System.out.println("Failed to establish connection to the benchmark system");
    }

}
