package request;

import entities.RawData;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import utils.Utils;

import java.util.List;

public class DataSource implements SourceFunction<RawData> {
    private static final long serialVersionUID = -1418183502579251444L;

    private static long numRequest;

    public DataSource(){
        DataSource.numRequest = -1;
    }

    public DataSource(long numRequest){
        DataSource.numRequest = numRequest;
    }

    @Override
    public void run(SourceContext<RawData> sourceContext) throws Exception {
        Query1Client query1 = new Query1Client("localhost","/data/1/");
        if (DataSource.numRequest == -1){
            while (true){
                String result = query1.getBatch();
                if (result == null){
                    break;
                }
                List<RawData> ls = Utils.parseJson(result);
                if (ls.size() == 0){
                    break;
                }
                for (RawData each: ls){
                    sourceContext.collectWithTimestamp(each, each.i);
                }
            }
        } else{
            long requestCount = 0;
            while (requestCount < numRequest){
                String result = query1.getBatch();
                if (result == null){
                    break;
                }
                List<RawData> ls = Utils.parseJson(result);
                if (ls.size() == 0){
                    break;
                }
                for (RawData each: ls){
                    synchronized (sourceContext.getCheckpointLock()) {
                        sourceContext.collectWithTimestamp(each, each.i);
                    }
                }
                requestCount++;
            }
        }
    }

    @Override
    public void cancel() {

    }
}
