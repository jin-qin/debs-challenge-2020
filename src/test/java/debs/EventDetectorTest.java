package debs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import entities.KeyedFeature;

import entities.RawData;
import org.apache.flink.streaming.api.watermark.Watermark;

import org.junit.Test;
import request.QueryClient;
import streaming.EventDetector;
import utils.Config;
import utils.Utils;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class EventDetectorTest {

    @Test
    public void test(){
        String serverIP = System.getenv("BENCHMARK_SYSTEM_URL");
        QueryClient query1 = new QueryClient(serverIP,"/data/1/");
        String result = query1.getBatch();
        for (Object obj:((JSONArray)JSON.parseObject(result).get("records"))){
            Long i = (Long)((JSONObject)obj).get("i");
            Double voltage = (Double)((JSONObject)obj).get("voltage");
            Double current = (Double)((JSONObject)obj).get("current");
            
        }
//        List<RawData> results = JSON.parseArray(JSON.parseObject(result).get("records").toString(), RawData.class);
//        for (Object each: results){
//            System.out.println(each);
//        }
//        System.out.println();

    }


    // @Test
    // public void updateClusteringTest(){
    //     List<KeyedFeature> ls = new ArrayList<>();
    //     ls.add(new KeyedFeature(0,0,0, 1, 2));
    //     ls.add(new KeyedFeature(0,1,1, 1.01, 2.02));
    //     ls.add(new KeyedFeature(0,2,2, 0.99, 2.03));
    //     ls.add(new KeyedFeature(0,3,3, 1.02, 1.97));

    //     ls.add(new KeyedFeature(0,4,4, 100, 300.1));
    //     ls.add(new KeyedFeature(0,5,5, 20.03, 4.97));
    //     ls.add(new KeyedFeature(0,5,5, 20.01, 4.98));

    //     ls.add(new KeyedFeature(0,5,5, 10.01, 5.02));
    //     ls.add(new KeyedFeature(0,6,6, 9.99, 4.99));
    //     ls.add(new KeyedFeature(0,5,5, 10.02, 5.01));
    //     ls.add(new KeyedFeature(0,6,6, 9.98, 4.98));

    //     EventDetector ed = new EventDetector();
    //     ed.updateClustering(ls);
    //     System.out.println(ed.getClusteringStructure());
    // }

    // @Test
    // public void updateClusteringTestFromTestData() throws Exception {
    //     System.out.println(System.getProperty("user.dir"));
    //     // check 352
    //     List<KeyedFeature> input352 = new ArrayList<>();
    //     JSONArray arr352 = (JSONArray) new JSONParser().parse(new FileReader("testdata/352.json"));
    //     for (Object obj: arr352) {
    //         if (obj instanceof JSONArray) {
    //             Object[] feature = ((JSONArray) obj).toArray();
    //             input352.add(new KeyedFeature(-1, -1, -1, (Double)feature[0], (Double)feature[1]));
    //         }
    //     }
    //     EventDetector ed352 = new EventDetector();
    //     ed352.updateClustering(input352);
    //     ed352.getClusteringStructure();

    //     // check 431
    //     List<KeyedFeature> input431 = new ArrayList<>();
    //     JSONArray arr431 = (JSONArray) new JSONParser().parse(new FileReader("testdata/431.json"));
    //     for (Object obj: arr431) {
    //         if (obj instanceof JSONArray) {
    //             Object[] feature = ((JSONArray) obj).toArray();
    //             input431.add(new KeyedFeature(-1, -1, -1, (Double)feature[0], (Double)feature[1]));
    //         }
    //     }
    //     EventDetector ed431 = new EventDetector();
    //     ed431.updateClustering(input431);
    //     ed431.getClusteringStructure();
    // }

    // @Test
    // public void computeAndEvaluateLossTest() throws Exception {
    //     // EventDetector ed = new EventDetector();
    // }
}
