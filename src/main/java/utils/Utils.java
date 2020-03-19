package utils;

import entities.RawData;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.ArrayList;
import java.util.List;

public class Utils {
    public static List<RawData> parseJson(String str){
        List<RawData> ls = new ArrayList<>();
        try {
            JSONObject obj = (JSONObject) new JSONParser().parse(str);
            JSONArray records = (JSONArray) obj.get("records");
            for (Object each: records){
                JSONObject jobj = (JSONObject)each;
                ls.add(new RawData(Integer.parseInt(jobj.get("i").toString()), Double.parseDouble(jobj.get("voltage").toString()), Double.parseDouble(jobj.get("current").toString())));
            }
            return ls;
        }catch (Exception e){
            e.printStackTrace();
        }
        return ls;
    }
}
