package utils;

import entities.RawData;

import java.util.List;

public class Metrics {

    public static double activePower(List<RawData> inputs){
        double sum = 0;
        for (RawData each: inputs){
            sum += each.voltage * each.current;
        }
        return sum / inputs.size();
    }

    public static double apparentPower(List<RawData> inputs){
        double vol_sq = 0;
        double cur_sq = 0;
        for (RawData each: inputs){
            vol_sq += Math.pow(each.voltage, 2);
            cur_sq += Math.pow(each.current, 2);
        }
        double result = Math.sqrt(vol_sq / inputs.size()) * Math.sqrt(cur_sq / inputs.size());
        return result;
    }

    public static double reactivePower(double s, double p){
        return Math.sqrt(Math.pow(s, 2) - Math.pow(p, 2));
    }
}
