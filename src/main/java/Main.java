import entities.DetectedEvent;
import entities.Feature;
import entities.RawData;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import request.DataSourceForQuery1;
import request.DataSourceForQuery2;
import request.QueryClient;
import streaming.QueryStreaming;
import utils.Utils;

public class Main {
    public static void main(String[] args) throws Exception{
        query1();

    }

    public static void query1() throws Exception{
        String serverIP = System.getenv("SERVER_IP");
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // start the data generator
        env.setParallelism(1);

        DataStream<RawData> input = env
                .addSource(new DataSourceForQuery1())
                .setParallelism(1);

        DataStream<Feature> features = Utils.computeInputSignal(input);
        DataStream<DetectedEvent> result = QueryStreaming.start(features);
//        result.print().setParallelism(1);
        QueryClient qc = new QueryClient(serverIP, "/data/1/");
        result.addSink(new SinkFunction<DetectedEvent>() {
            private static final long serialVersionUID = 192888109083989331L;
            private long postCounter;

            @Override
            public void invoke(DetectedEvent value, Context context) throws Exception {
                System.out.println(value);
//                qc.post(value);
                QueryClient.post(value);
                postCounter += 1;
                if (postCounter == 502){
                    QueryClient.finalGet();
                }
            }

        }).setParallelism(1);



        env.execute("DEBS Challenge 2020");
    }

    public void query2() throws Exception{
        String serverIP = System.getenv("SERVER_IP");
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // start the data generator
        env.setParallelism(1);

        DataStream<RawData> input = env
                .addSource(new DataSourceForQuery2())
                .setParallelism(1);

        DataStream<Feature> features = Utils.computeInputSignal(input);
        DataStream<DetectedEvent> result = QueryStreaming.start(features);
//        result.print().setParallelism(1);
        QueryClient qc = new QueryClient(serverIP, "/data/2/");
        result.addSink(new SinkFunction<DetectedEvent>() {
            private static final long serialVersionUID = 192888109083989331L;
            private long postCounter;

            @Override
            public void invoke(DetectedEvent value, Context context) throws Exception {
                System.out.println(value);
//                qc.post(value);
                // TODO: bug
                QueryClient.post(value);
                postCounter += 1;
                if (postCounter == 500){
                    QueryClient.finalGet();
                }
            }

        }).setParallelism(1);



        env.execute("DEBS Challenge 2020");
    }

}
