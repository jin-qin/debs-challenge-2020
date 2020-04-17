package debs;

import org.junit.Test;

public class StreamTest {

//     @Test
//     public void keyedFeatureTest() throws Exception {
//         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//         env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//         // start the data generator
//         DataStream<RawData> input = env
//                 .addSource(new DataSource())
//                 .setParallelism(1);

//         DataStream<Feature> features = Utils.computeInputSignal(input);
//         DataStream<DetectedEvent> result = Query1Streaming.start(features);
//         result.print();
//         env.execute("Number of busy machines every 5 minutes over the last 15 minutes");
//     }

//     @Test
//     public void predictTest() throws Exception{
//         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//         env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//         // start the data generator
//         DataStream<RawData> input = env
//                 .addSource(new DataSource(500))
//                 .setParallelism(1);

//         DataStream<Feature> features = Utils.computeInputSignal(input);
//         DataStream<DetectedEvent> result = Query1Streaming.start(features);
// //        result.print();
//         env.execute("Number of busy machines every 5 minutes over the last 15 minutes");
//     }

    @Test
    public void testPostMethod() throws Exception{

    }

}
