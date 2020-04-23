import entities.DetectedEvent;
import entities.Feature;
import entities.RawData;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.*;

import org.apache.commons.math3.analysis.function.Exp;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import request.DataSourceForQuery1;
import request.DataSourceForQuery2;
import request.QueryClient;
import streaming.QueryStreaming1;
import streaming.QueryStreaming2;
import utils.Config;
import utils.Utils;

public class Main {
    static {
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
    }

    private static Queue<DetectedEvent> query1PostQueue = new LinkedList<>();
    private static Queue<DetectedEvent> query2PostQueue = new LinkedList<>();

    public static void main(String[] args) throws Exception {
//        setRedirectOutputToLogFile();

        Utils.waitForBenchmarkSystem(System.getenv("BENCHMARK_SYSTEM_URL"), 80);
        query1();
        System.out.println("Query 1 finished");
        query2();
        System.out.println("Query 2 finished");
    }

    public static void setRedirectOutputToLogFile() throws FileNotFoundException {
        File file = new File(Config.log_file_path);
        PrintStream stream = new PrintStream(file);
        System.setOut(stream);
    }

    public static void query1() throws Exception{
        String serverIP = System.getenv("BENCHMARK_SYSTEM_URL");


        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // start the data generator
        env.setParallelism(1);

        DataStream<RawData> input = env
                .addSource(new DataSourceForQuery1())
                .setParallelism(1);

        DataStream<Feature> features = Utils.computeInputSignal(input);
        DataStream<DetectedEvent> result = QueryStreaming1.start(features);

        QueryClient.setParams(serverIP, "/data/1/");
        result.addSink(new SinkFunction<DetectedEvent>() {
            private static final long serialVersionUID = 192888109083989331L;

            @Override
            public void invoke(DetectedEvent value, Context context) throws Exception {
//                if (Config.debug)
//                    System.out.println(value);
                synchronized (query1PostQueue){
                    query1PostQueue.add(value);
                }
//                if (Config.debug)
//                    System.out.println(query1PostQueue.size());
            }

        }).setParallelism(1);

        Thread postThread = new Thread(){
            @Override
            public void run() {
//                super.run();
                System.out.println("enter thread");
                while(true){
//                    System.out.println(query1PostQueue.size());
//                    try {
//                        Thread.sleep(1);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                    if (query1PostQueue.size() > 0){
                        DetectedEvent item;
                        synchronized (query1PostQueue){
                            item =query1PostQueue.poll();
                        }
                        if (Config.debug)
                            System.out.println(item);
                        try {
//                            QueryClient.post(item);
                            if (item.getBatchCounter() * Config.w1_size == Config.endofStream){
                                QueryClient.finalGet();
                                break;
                            }
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                    }
                }
            }
        };
        postThread.start();
        postThread.join();
        System.out.println(postThread);


        env.execute("DEBS Challenge 2020 - Query 1");
    }

    public static void query2() throws Exception{
        String serverIP = System.getenv("BENCHMARK_SYSTEM_URL");
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // start the data generator
        env.setParallelism(1);

        DataStream<RawData> input = env
                .addSource(new DataSourceForQuery2())
                .setParallelism(1);

        DataStream<Feature> features = Utils.computeInputSignal(input);
        DataStream<DetectedEvent> result = QueryStreaming2.start(features);
        QueryClient.setParams(serverIP, "/data/2/");
        result.addSink(new SinkFunction<DetectedEvent>() {
            private static final long serialVersionUID = 192888109083989331L;

            @Override
            public void invoke(DetectedEvent value, Context context) throws Exception {
                query2PostQueue.add(value);
            }

        }).setParallelism(1);

        Thread postThread = new Thread(){
            @Override
            public void run() {
//                super.run();
                while(true){
                    if (query2PostQueue.size() > 0){
                        DetectedEvent item =query2PostQueue.poll();
                        if (Config.debug)
                            System.out.println(item);
                        try {
                            QueryClient.post(item);
                            if (item.getBatchCounter() * Config.w1_size == Config.endofStream){
                                QueryClient.finalGet();
                                break;
                            }
                        }catch(Exception e){
                            e.printStackTrace();
                        }
                    }
                }
            }
        };
        postThread.start();

        env.execute("DEBS Challenge 2020 - Query 2");
    }
}
