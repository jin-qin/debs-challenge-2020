package request;

import entities.RawData;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import utils.Config;
import utils.Utils;

import java.util.List;

public class DataSourceForQuery2 implements SourceFunction<RawData> {
    private static final long serialVersionUID = -1418183502579251444L;

    public DataSourceForQuery2(){
    }

    @Override
    public void run(SourceContext<RawData> sourceContext) throws Exception {
        String serverIP = System.getenv("SERVER_IP");
        QueryClient query1 = new QueryClient(serverIP,"/data/2/");
        if (Config.num_records == -1){
            long current_time = 0;
            while (true){
                String result = query1.getBatch();
                if (result == null){
                    break;
                }
                List<RawData> ls = Utils.parseJson(result, current_time);
                if (ls.size() == 0){
                    break;
                }

                for (RawData each: ls){
                    sourceContext.collectWithTimestamp(each, each.i);

                }
                long watermark_time = current_time - Config.max_latency;
                if (watermark_time >= 0){
                    sourceContext.emitWatermark(new Watermark(watermark_time * Config.w1_size));
                }
                current_time += 1;
            }
        }
//        else{
//            long requestCount = 0;
//            while (requestCount < Config.num_records){
//                String result = query1.getBatch();
//                if (result == null){
//                    break;
//                }
//                List<RawData> ls = Utils.parseJson(result);
//                if (ls.size() == 0){
//                    break;
//                }
//                for (RawData each: ls){
//                    synchronized (sourceContext.getCheckpointLock()) {
//                        sourceContext.collectWithTimestamp(each, each.i);
//                    }
//                }
//                requestCount++;
//            }
//        }
    }

    @Override
    public void cancel() {

    }
}
