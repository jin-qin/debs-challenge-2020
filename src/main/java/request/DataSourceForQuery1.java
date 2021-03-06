package request;

import entities.RawData;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import utils.Config;
import utils.Utils;

import java.util.List;

public class DataSourceForQuery1 implements SourceFunction<RawData> {
    private static final long serialVersionUID = -1418183502579251444L;

    public DataSourceForQuery1(){
    }

    @Override
    public void run(SourceContext<RawData> sourceContext) throws Exception {
       String serverIP = System.getenv("BENCHMARK_SYSTEM_URL");
        QueryClient query1 = new QueryClient(serverIP,"/data/1/");
        if (Config.num_records == -1){
            long current_time = 0;
            long lastWatermark = 0;
            while (true){
                String result = query1.getBatch();
                if (result == null){
                    break;
                }
//                if (current_time == 800){
//                    System.out.println("breakpoint");
//                }
                List<RawData> ls = Utils.parseJson(result, current_time);
//                System.out.printf("%d, %s\n", current_time, ls.get(0).toString());
                if (ls.size() == 0){
                    for (;lastWatermark<current_time*Config.w1_size; lastWatermark+=Config.w1_size ){
                        sourceContext.emitWatermark(new Watermark(lastWatermark));
                    }
                    Config.endofStream = lastWatermark - Config.w1_size;
                    sourceContext.emitWatermark(Watermark.MAX_WATERMARK);
                    break;
                }

                for (RawData each: ls){
                    sourceContext.collectWithTimestamp(each, each.i);

                }
                long watermark_time = current_time;
                if (watermark_time >= 0){
//                    System.out.printf("current watermark: %d\n", watermark_time * Config.w1_size);
                    sourceContext.emitWatermark(new Watermark(watermark_time * Config.w1_size));
                    lastWatermark = watermark_time * Config.w1_size;
                }
                current_time += 1;
            }
        }
    }

    @Override
    public void cancel() {

    }
}
