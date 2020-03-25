import entities.DetectedEvent;
import entities.Event;
import entities.Point;
import entities.RawData;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import request.DataSource;

public class Main {

    public static void main(String[] args) throws Exception{
        // set up query connection

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<RawData> input = env
                .addSource(new DataSource())
                .setParallelism(1);
        
        EventDector ed = new EventDector();

        DataStream<Point> features = ed.computeInputSignal(input);
        DataStream<DetectedEvent> detectEvents = ed.predict(features);
        
        env.execute("Number of busy machines every 5 minutes over the last 15 minutes");

    }

}
