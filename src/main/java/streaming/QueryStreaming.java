package streaming;

import entities.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import scala.concurrent.java8.FuturesConvertersImpl;
import utils.Config;

public class QueryStreaming {
    public static DataStream<DetectedEvent> start(DataStream<Feature> features) {
        DataStream<DetectedEvent> result = features.flatMap(new AddKeyMapper())
                .keyBy(e -> e.key)
                .process(new PredictFunc())
                .keyBy(e -> e.isEventDetected())
                .process(new SortFunc())
                .setParallelism(1)
                .process(new VerifyFunc())
                .setParallelism(1);
        return result;
    }

    public static AddKeyMapper newAddKeyMapper() {
        return newAddKeyMapper();
    }
}

class AddKeyMapper implements FlatMapFunction<Feature, KeyedFeature> {
    private static final long serialVersionUID = 192888106183989331L;

    @Override
    public void flatMap(Feature value, Collector<KeyedFeature> out) throws Exception {

        long offset = value.idx % Config.partion_size;
        long partionIdx = value.idx / Config.partion_size; // return the floor long value
        KeyedFeature keyed = new KeyedFeature(partionIdx, offset, value.idx, value.f1, value.f2, value.size);
        out.collect(keyed);
        if (offset < Config.w2_size && partionIdx != 0) {
            KeyedFeature keyedAdd = new KeyedFeature(partionIdx - 1, Config.partion_size + offset, value.idx, value.f1, value.f2, value.size);
            out.collect(keyedAdd);
        }
    }
}

class PredictFunc extends KeyedProcessFunction<Long, KeyedFeature, DetectedEvent> {
    private static final long serialVersionUID = -5973216181346355124L;

    private HashMap<Long, Window2> w2Map = new HashMap<>();
    private HashMap<Long, EventDetector> edMap = new HashMap<>();
    private HashMap<Long, Long> windowStartIndexMap = new HashMap<>();
    private HashMap<Long, Long> currentWindowStartMap = new HashMap<>();
    private HashMap<Long, HashMap<Long, KeyedFeature>> mapTsFeatureMap = new HashMap<>();

    private void initStates(Long key) {
        if (w2Map.get(key) == null) {
            w2Map.put(key, new Window2());
        }
        if (edMap.get(key) == null) {
            edMap.put(key, new EventDetector());
        }
        if (mapTsFeatureMap.get(key) == null) {
            mapTsFeatureMap.put(key, new HashMap<>());
        }
    }

    @Override
    public void processElement(KeyedFeature feature, Context context, Collector<DetectedEvent> collector) throws Exception {
        Long key = context.getCurrentKey();
        initStates(key);

        long ts = context.timestamp();
        HashMap<Long, KeyedFeature> mapTsFeature = mapTsFeatureMap.get(key);
        mapTsFeature.put(ts, feature);
        mapTsFeatureMap.put(key, mapTsFeature);
        context.timerService().registerEventTimeTimer(ts);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DetectedEvent> out) throws Exception {
        Long key = ctx.getCurrentKey();

        HashMap<Long, KeyedFeature> mapTsFeature = mapTsFeatureMap.get(key);
        KeyedFeature feature = mapTsFeature.get(timestamp);
        mapTsFeature.remove(timestamp);
        mapTsFeatureMap.put(key, mapTsFeature);

        Window2 w2_v = w2Map.get(key);
        if (w2_v.size() <= 0) {
            windowStartIndexMap.put(key, feature.offset);
            currentWindowStartMap.put(key, feature.offset);
        }
        w2_v.addElement(feature);
        w2Map.put(key, w2_v);
        
        PredictedEvent e = edMap.get(key).predict(w2_v);

        if (e == null) {
            if (w2_v.size() > Config.w2_size) {
                w2_v.clear();
                w2Map.put(key, w2_v);
            }

            if (feature.key > 0 && feature.offset < Config.w2_size) return;

            out.collect(new DetectedEventToVerify(feature.idx, false, -1, feature, -1, -1));
            return;
        }

        int meanEventIndex = (e.eventStart + e.eventEnd) / 2;

        List<KeyedFeature> subWindow = w2_v.subWindow(e.eventEnd, w2_v.size());
        w2_v.setW2(subWindow);
        w2Map.put(key, w2_v);

        Long windowStartIndex = windowStartIndexMap.get(key);
        windowStartIndexMap.put(key, windowStartIndex + e.eventEnd);
        Long globalCurrentWindowStart = feature.key * Config.partion_size + currentWindowStartMap.get(key);
        currentWindowStartMap.put(key, windowStartIndexMap.get(key));

        if (feature.key > 0 && feature.offset < Config.w2_size) return;

        out.collect(new DetectedEventToVerify(feature.idx, true, globalCurrentWindowStart + meanEventIndex + 1, feature, globalCurrentWindowStart + e.eventStart, globalCurrentWindowStart + e.eventEnd));
    }
}

//

class VerifyFunc extends ProcessFunction<DetectedEvent, DetectedEvent> {
    private static final long serialVersionUID = -3260627464897649644L;

    private HashMap<Long, DetectedEventToVerify> detectedEvents = new HashMap<>();
    private List<Long> partionSchedule = new ArrayList<>();
    private List<DetectedEventToVerify> toVerifyEvents = new ArrayList<>();
    private OutputQueue outputQueue = new OutputQueue();
    private VerifyQueue buffered = new VerifyQueue();
    private long currentOutputBound = 0;
    private long currentWindowStart = 0;
    private long INTERVAL = Config.w2_size + 1;
    private long currentKey = 0;

    @Override
    public void processElement(DetectedEvent value, Context ctx, Collector<DetectedEvent> out) throws Exception {
        DetectedEventToVerify evt = (DetectedEventToVerify) value;
        buffered.addElement(evt.getFeature());
        outputQueue.addElement(evt);
//        out.collect(value);

        if (currentKey == (evt.getFeature().key - 1)){
            currentKey += 1;
        }

        if (evt.isEventDetected()){
            if (evt.getFeature().key == currentKey) {
                if (toVerifyEvents.size() > 0){
                    if (detectedEvents.size() > 0){
                        partionSchedule.add(Collections.max(detectedEvents.keySet()));
                    }else{
                        partionSchedule.add(evt.getFeature().idx);
                    }
                }
                currentKey += 1;
                long nxtId = evt.getEventEnd() + 2*this.INTERVAL;
                this.detectedEvents.put(nxtId, evt);
            }else{
                long nxtId = evt.getEventEnd() + 2*this.INTERVAL;
                this.detectedEvents.put(nxtId, evt);
            }
        }else{
            if (evt.getFeature().key == currentKey){
                this.toVerifyEvents.add(evt);
            }else{
                ;
            }
        }

        //ontimer
        if (toVerifyEvents.size() == 4 * this.INTERVAL){
            while (!(toVerifyEvents.get(0).getFeature().idx >= currentWindowStart && toVerifyEvents.get(0).getFeature().idx < currentWindowStart + this.INTERVAL)){
                currentWindowStart += this.INTERVAL;
            }
            List<KeyedFeature> features = buffered.subWindow(currentWindowStart, toVerifyEvents.get(toVerifyEvents.size() -1).getFeature().idx+1);
            Query1Dectector q1d = new Query1Dectector(features);
            List<Tuple2<DetectedEvent, Long>> result = q1d.dectedEvent2();
            for (Tuple2<DetectedEvent, Long> each: result){
                outputQueue.update(each.f0.getBatchCounter(), each.f0);
                currentWindowStart = each.f1;
            }

            // output buffered events
            currentOutputBound = toVerifyEvents.get(toVerifyEvents.size() -1).getBatchCounter() + 1;
            List<DetectedEvent> outputLs = outputQueue.outputToBound(currentOutputBound);
            for (DetectedEvent outEvt: outputLs) {
                out.collect(outEvt);
            }
            toVerifyEvents.clear();
        }

        if (detectedEvents.keySet().contains(evt.getFeature().idx)){
            DetectedEventToVerify detectedEvt = detectedEvents.get(evt.getFeature().idx);
            detectedEvents.remove(evt.getFeature().idx);

            while (!(detectedEvt.getEventStart() >= currentWindowStart && detectedEvt.getEventEnd() < currentWindowStart + this.INTERVAL)){
                currentWindowStart += this.INTERVAL;
            }

            List<KeyedFeature> features = buffered.subWindow((int)currentWindowStart, (int)(currentWindowStart + 2*this.INTERVAL));
            Query1Dectector q1d = new Query1Dectector(features);
            Tuple2<DetectedEvent, Long> result = q1d.dectedEvent();
            outputQueue.update(detectedEvt.getBatchCounter(), new DetectedEvent(detectedEvt.getBatchCounter(), false, -1));
//            out.collect(new DetectedEvent(detectedEvt.getBatchCounter(), false, -1));
            if (result != null){
                outputQueue.update(result.f0.getBatchCounter(), result.f0);
//                out.collect(result.f0);
                currentWindowStart = result.f1;
                currentOutputBound = currentWindowStart;
            }
            List<DetectedEvent> outputLs = outputQueue.outputToBound(currentOutputBound);
            for (DetectedEvent outEvt: outputLs) {
                out.collect(outEvt);
            }
        }

        if (partionSchedule.contains(evt.getFeature().idx)){
            // remove all already verified events
            List<DetectedEventToVerify> temp = new ArrayList<>();
            for (DetectedEventToVerify each: toVerifyEvents) {
                if (each.getFeature().idx < currentKey){
                    temp.add(each);
                }
            }
            toVerifyEvents.removeAll(temp);

            // move currentWindowStart to right place
            while (!(toVerifyEvents.get(0).getFeature().idx >= currentWindowStart && toVerifyEvents.get(0).getFeature().idx < currentWindowStart + this.INTERVAL)){
                currentWindowStart += this.INTERVAL;
            }

            // verify the events
            List<KeyedFeature> features = buffered.subWindow((int)currentWindowStart, toVerifyEvents.get(toVerifyEvents.size() -1).getFeature().idx+1);
            Query1Dectector q1d = new Query1Dectector(features);
            List<Tuple2<DetectedEvent, Long>> result = q1d.dectedEvent2();
            for (Tuple2<DetectedEvent, Long> each: result){
                outputQueue.update(each.f0.getBatchCounter(), each.f0);
//                out.collect(each.f0);
                currentWindowStart = each.f1;
            }

            // output buffered events
            currentOutputBound = toVerifyEvents.get(toVerifyEvents.size() -1).getBatchCounter() + 1;
            List<DetectedEvent> outputLs = outputQueue.outputToBound(currentOutputBound);
            for (DetectedEvent outEvt: outputLs) {
                out.collect(outEvt);
            }
            toVerifyEvents.clear();
        }

        if (Config.endofStream != -1 && evt.getBatchCounter() == Config.endofStream/Config.w1_size){
            // detectedEvents clear
            if (detectedEvents.size()>0){
                DetectedEventToVerify detectedEvt = detectedEvents.values().iterator().next();
                while (!(detectedEvt.getEventStart() >= currentWindowStart && detectedEvt.getEventEnd() < currentWindowStart + this.INTERVAL)){
                    currentWindowStart += this.INTERVAL;
                }

                List<KeyedFeature> features = buffered.subWindow((int)currentWindowStart, (int)(evt.getFeature().idx + 1));
                Query1Dectector q1d = new Query1Dectector(features);
                Tuple2<DetectedEvent, Long> result = q1d.dectedEvent();
                outputQueue.update(detectedEvt.getBatchCounter(), new DetectedEvent(detectedEvt.getBatchCounter(), false, -1));
//            out.collect(new DetectedEvent(detectedEvt.getBatchCounter(), false, -1));
                if (result != null){
                    outputQueue.update(result.f0.getBatchCounter(), result.f0);
//                out.collect(result.f0);
                    currentWindowStart = result.f1;
                    currentOutputBound = currentWindowStart;
                }
                List<DetectedEvent> outputLs = outputQueue.outputToBound(currentOutputBound);
                for (DetectedEvent outEvt: outputLs) {
                    out.collect(outEvt);
                }
            }
            else if (toVerifyEvents.size() > 0){
                // clear toVerifyArray
                // remove all already verified events
                List<DetectedEventToVerify> temp = new ArrayList<>();
                for (DetectedEventToVerify each: toVerifyEvents) {
                    if (each.getFeature().idx < currentKey){
                        temp.add(each);
                    }
                }
                toVerifyEvents.removeAll(temp);

                // move currentWindowStart to right place
                while (!(toVerifyEvents.get(0).getFeature().idx >= currentWindowStart && toVerifyEvents.get(0).getFeature().idx < currentWindowStart + this.INTERVAL)){
                    currentWindowStart += this.INTERVAL;
                }

                // verify the events
                List<KeyedFeature> features = buffered.subWindow((int)currentWindowStart, toVerifyEvents.get(toVerifyEvents.size() -1).getFeature().idx+1);
                Query1Dectector q1d = new Query1Dectector(features);
                List<Tuple2<DetectedEvent, Long>> result = q1d.dectedEvent2();
                for (Tuple2<DetectedEvent, Long> each: result){
                    outputQueue.update(each.f0.getBatchCounter(), each.f0);
//                out.collect(each.f0);
                    currentWindowStart = each.f1;
                }

                // output buffered events
                currentOutputBound = toVerifyEvents.get(toVerifyEvents.size() -1).getBatchCounter() + 1;
                List<DetectedEvent> outputLs = outputQueue.outputToBound(currentOutputBound);
                for (DetectedEvent outEvt: outputLs) {
                    out.collect(outEvt);
                }
                toVerifyEvents.clear();
            }

            Collection<DetectedEvent> ls = outputQueue.outputAll();
            for (DetectedEvent each: ls){
                out.collect(each);
            }
        }
    }
}

class SortFunc extends KeyedProcessFunction<Boolean, DetectedEvent, DetectedEvent> {
    private static final long serialVersionUID = -3260627464897625644L;

    private MapState<Long, DetectedEvent> mapTsEvent;

    @Override
    public void open(Configuration config) throws IOException {
        MapStateDescriptor<Long, DetectedEvent> descriptorMapTsEvent = 
                new MapStateDescriptor<Long, DetectedEvent>(
                    "MapTsEvent", 
                    Long.TYPE, 
                    DetectedEvent.class);
        mapTsEvent = getRuntimeContext().getMapState(descriptorMapTsEvent);
    }

    @Override
    public void processElement(DetectedEvent value, Context ctx, Collector<DetectedEvent> out) throws Exception {
        mapTsEvent.put(ctx.timestamp(), value);
        ctx.timerService().registerEventTimeTimer(ctx.timestamp());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DetectedEvent> out) throws Exception {
        out.collect(mapTsEvent.get(timestamp));
        mapTsEvent.remove(timestamp);
    }
}


class SortFlatMapper implements FlatMapFunction<DetectedEvent, DetectedEvent>{
    private static final long serialVersionUID = 5840447412541874914L;

    private PriorityQueue<DetectedEvent> pqueue = new PriorityQueue<>(new DetectedEventComparator());
    private long nextIdx = 0;

    @Override
    public void flatMap(DetectedEvent detectedEvent, Collector<DetectedEvent> collector) throws Exception {
        pqueue.add(detectedEvent);
        while(pqueue.size() > 0 && pqueue.peek().getBatchCounter() == nextIdx){
            DetectedEvent e = pqueue.poll();
            collector.collect(e);
            nextIdx++;
        }
    }
}

class DetectedEventComparator implements Comparator<DetectedEvent>, Serializable { 
    private static final long serialVersionUID = 4361874715073716094L;

    public DetectedEventComparator() { }

    public int compare(DetectedEvent e1, DetectedEvent e2) 
    {
        long v1 = e1.getBatchCounter();
        long v2 = e2.getBatchCounter();
        if (v1 < v2) return 1;
        if (v1 == v2) return 0;
        return -1;
    } 
}
