package request;

import entities.RawData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import utils.Utils;

import java.util.List;

public class DataSource implements SourceFunction<RawData> {
    private static final long serialVersionUID = -1418183502579251444L;

    @Override
    public void run(SourceContext<RawData> sourceContext) throws Exception {
        Query1Client query1 = new Query1Client("localhost","/data/1");
        int max = 30;
        int i=0;
        while (true){
            String result = query1.getBatch();
            List<RawData> ls = Utils.parseJson(result);
            System.out.println("finished one request");
            for (RawData each: ls){
                sourceContext.collectWithTimestamp(each, each.i);
            }
            if (i==10){
                break;
            }
            i++;
        }
    }

    @Override
    public void cancel() {

    }
}
