package streaming;

import entities.DetectedEvent;
import entities.Feature;
import entities.KeyedFeature;
import entities.PredictedEvent;
import entities.Window2;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import utils.Config;

public class Query1Streaming {
    public static void start(DataStream<Feature> features) {
        features.flatMap(new AddKeyMapper())
                .keyBy(e -> e.key)
                .map(new PredictMapper());
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
            KeyedFeature keyedAdd = new KeyedFeature(partionIdx - 1, offset, value.idx, value.f1, value.f2);
            out.collect(keyedAdd);
        }
    }
}

class PredictMapper extends RichMapFunction<KeyedFeature, DetectedEvent> {
    private static final long serialVersionUID = -5973216181346355124L;

    private ValueState<Window2> w2;
    private ValueState<EventDector> ed;

    private ValueState<Integer> windowStartIndex;
    private ValueState<Integer> currentWindowStart;
    private ValueState<Integer> batchCounter;

    @Override
    public void open(Configuration config) throws IOException {
        ValueStateDescriptor<Window2> descriptorW2 =
                new ValueStateDescriptor<>(
                        "window2", // the state name
                        TypeInformation.of(new TypeHint<Window2>() {})); // type information
        w2 = getRuntimeContext().getState(descriptorW2);
        w2.update(new Window2());

        ValueStateDescriptor<EventDector> descriptorEventDector =
                new ValueStateDescriptor<>(
                        "EventDector", // the state name
                        TypeInformation.of(new TypeHint<EventDector>() {})); // type information
        ed = getRuntimeContext().getState(descriptorEventDector);
        ed.update(new EventDector());

        ValueStateDescriptor<Integer> descriptorWindowStartIndex =
                new ValueStateDescriptor<>(
                        "WindowStartIndex", // the state name
                        TypeInformation.of(new TypeHint<Integer>() {})); // type information
        windowStartIndex = getRuntimeContext().getState(descriptorWindowStartIndex);
        windowStartIndex.update(1); // why 1?

        ValueStateDescriptor<Integer> descriptorCurrentWindowStart =
                new ValueStateDescriptor<>(
                        "CurrentWindowStart", // the state name
                        TypeInformation.of(new TypeHint<Integer>() {})); // type information
        currentWindowStart = getRuntimeContext().getState(descriptorCurrentWindowStart);
        currentWindowStart.update(1); // why 1?

        ValueStateDescriptor<Integer> descriptorBatchCounter=
                new ValueStateDescriptor<>(
                        "BatchCounter", // the state name
                        TypeInformation.of(new TypeHint<Integer>() {})); // type information
        batchCounter = getRuntimeContext().getState(descriptorBatchCounter);
        batchCounter.update(0);
    }

    @Override
    public DetectedEvent map(KeyedFeature feature) throws Exception {
        batchCounter.update(batchCounter.value() + 1);

        Window2 w2_v = w2.value();
        w2_v.addElement(feature);
        w2.update(w2_v);

        PredictedEvent e = ed.value().predict(w2.value());

        if (e == null)
            return new DetectedEvent(batchCounter.value(), false, -1);

        int meanEventIndex = (e.eventStart + e.eventEnd) / 2;

        List<KeyedFeature> subWindow = w2.value().subWindow(e.eventEnd, w2.value().size());
        w2_v.setW2(subWindow);
        w2.update(w2_v);

        windowStartIndex.update(windowStartIndex.value() + e.eventEnd);
        currentWindowStart.update(windowStartIndex.value());

        return new DetectedEvent(batchCounter.value(), true, currentWindowStart.value() + meanEventIndex);
    }
}
