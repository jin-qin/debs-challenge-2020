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
//        inputStream.print();
        EventDector ed = new EventDector();

        DataStream<Point> stage1 = ed.computeInputSignal(input);
//        DataStream<Event> result = ed.predict(stage1);

//        result.print();
        env.execute("Number of busy machines every 5 minutes over the last 15 minutes");

    }

}
