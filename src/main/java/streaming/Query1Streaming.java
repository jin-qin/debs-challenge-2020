package streaming;

import entities.DetectedEvent;
import entities.Feature;
import entities.KeyedFeature;
import entities.PredictedEvent;
import entities.Window2;

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import utils.Config;

public class Query1Streaming {
    public static DataStream<DetectedEvent> start(DataStream<Feature> features) {
        DataStream<DetectedEvent> result = features.flatMap(new AddKeyMapper())
                .keyBy(e -> e.key)
                .process(new PredictFunc());
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
        KeyedFeature keyed = new KeyedFeature(partionIdx, offset, value.idx, value.f1, value.f2);
        out.collect(keyed);
        if (offset < Config.w2_size && partionIdx != 0) {
            KeyedFeature keyedAdd = new KeyedFeature(partionIdx - 1, Config.partion_size + offset, value.idx, value.f1, value.f2);
            out.collect(keyedAdd);
        }
    }
}

class PredictFunc extends KeyedProcessFunction<Long, KeyedFeature, DetectedEvent> {
    private static final long serialVersionUID = -5973216181346355124L;

    private ValueState<Window2> w2;
    private ValueState<EventDector> ed;

    private ValueState<Long> windowStartIndex;
    private ValueState<Long> currentWindowStart;

    private MapState<Long, KeyedFeature> mapTsFeature;

    @Override
    public void open(Configuration config) throws IOException {
        ValueStateDescriptor<Window2> descriptorW2 =
                new ValueStateDescriptor<>(
                        "window2", // the state name
                        TypeInformation.of(new TypeHint<Window2>() {})); // type information
        w2 = getRuntimeContext().getState(descriptorW2);

        ValueStateDescriptor<EventDector> descriptorEventDector =
                new ValueStateDescriptor<>(
                        "EventDector", // the state name
                        TypeInformation.of(new TypeHint<EventDector>() {})); // type information
        ed = getRuntimeContext().getState(descriptorEventDector);

        ValueStateDescriptor<Long> descriptorWindowStartIndex =
                new ValueStateDescriptor<>(
                        "WindowStartIndex", // the state name
                        TypeInformation.of(new TypeHint<Long>() {})); // type information
        windowStartIndex = getRuntimeContext().getState(descriptorWindowStartIndex);
        
        ValueStateDescriptor<Long> descriptorCurrentWindowStart =
                new ValueStateDescriptor<>(
                        "CurrentWindowStart", // the state name
                        TypeInformation.of(new TypeHint<Long>() {})); // type information
        currentWindowStart = getRuntimeContext().getState(descriptorCurrentWindowStart);

        MapStateDescriptor<Long, KeyedFeature> descriptorMapTsEvent = 
                new MapStateDescriptor<Long, KeyedFeature>(
                    "MapTsFeature", 
                    Long.TYPE, 
                    KeyedFeature.class);
        mapTsFeature = getRuntimeContext().getMapState(descriptorMapTsEvent);
    }

    @Override
    public void processElement(KeyedFeature feature, Context context, Collector<DetectedEvent> collector) throws Exception {
        if (w2.value() == null) w2.update(new Window2());
        if (ed.value() == null) ed.update(new EventDector());
        if (windowStartIndex.value() == null) windowStartIndex.update(feature.idx + 1); // why 1?
        if (currentWindowStart.value() == null) currentWindowStart.update(feature.idx + 1); // why 1?

        long ts = context.timestamp();
        mapTsFeature.put(ts, feature);
        context.timerService().registerEventTimeTimer(ts);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<DetectedEvent> out) throws Exception {
        KeyedFeature feature = mapTsFeature.get(timestamp);
        mapTsFeature.remove(timestamp);

        Window2 w2_v = w2.value();
        // if (w2_v.size() <= 0) {
        //     windowStartIndex.update(feature.idx + 1);
        //     currentWindowStart.update(feature.idx + 1);
        // }
        w2_v.addElement(feature);
        w2.update(w2_v);

        if (Config.debug) {
            if (feature.idx == 317) {
                System.out.println(">>>> 317 stage");
            }
        }
        
        PredictedEvent e = ed.value().predict(w2.value());

        if (e == null) {
            if (w2_v.size() > Config.w2_size) {
                // w2_v.clear();
                w2_v.removeFirst();
                w2.update(w2_v);

                windowStartIndex.update(windowStartIndex.value() + 1);
                currentWindowStart.update(windowStartIndex.value() + 1);
            }

            if (feature.key > 0 && feature.offset < Config.w2_size) return;

            out.collect(new DetectedEvent(feature.idx, false, -1));
            return;
        }

        int meanEventIndex = (e.eventStart + e.eventEnd) / 2;

        List<KeyedFeature> subWindow = w2.value().subWindow(e.eventEnd, w2.value().size());
        w2_v.setW2(subWindow);
        w2.update(w2_v);

        windowStartIndex.update(windowStartIndex.value() + e.eventEnd);
        Long tmpCurrentWindowStart = currentWindowStart.value();
        currentWindowStart.update(windowStartIndex.value());

        if (feature.key > 0 && feature.offset < Config.w2_size) return;

        out.collect(new DetectedEvent(feature.idx, true, tmpCurrentWindowStart + meanEventIndex));
    }
}

class SortFlatMapper implements FlatMapFunction<DetectedEvent, DetectedEvent>{
    private static final long serialVersionUID = 5840447412541874914L;

    private static PriorityQueue<DetectedEvent> pqueue = new PriorityQueue<>(new DetectedEventComparator());
    private static long nextIdx = 0;

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
