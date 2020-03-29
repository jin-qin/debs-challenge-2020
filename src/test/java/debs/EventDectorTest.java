package debs;

import entities.KeyedFeature;
import org.junit.Test;
import streaming.EventDector;

import java.util.ArrayList;
import java.util.List;

public class EventDectorTest {

    @Test
    public void updateClusteringTest(){
        List<KeyedFeature> ls = new ArrayList<>();
        ls.add(new KeyedFeature(0,0,1, 5.58861576, 5.6783343));
        ls.add(new KeyedFeature(0,0,2, 5.61514278, 5.67889839));
        ls.add(new KeyedFeature(0,0,3, 5.58673001, 5.66157244));
        ls.add(new KeyedFeature(0,0,4, 5.60168791, 5.71225114));
        ls.add(new KeyedFeature(0,0,5, 5.60340597, 5.67988757));
        ls.add(new KeyedFeature(0,0,6, 5.57902017, 5.68761717));
        ls.add(new KeyedFeature(0,0,7, 5.60759445, 5.69916852));
        ls.add(new KeyedFeature(0,0,8, 5.60618247, 5.67543936));
        ls.add(new KeyedFeature(0,0,9, 5.61093065, 5.69987675));
        ls.add(new KeyedFeature(0,0,10, 5.62488714, 5.7342662));
        ls.add(new KeyedFeature(0,0,11, 5.95865855, 5.67145262));
        ls.add(new KeyedFeature(0,0,12, 5.59275976, 5.63070964));
        ls.add(new KeyedFeature(0,0,13, 5.56155995, 5.57122507));
        ls.add(new KeyedFeature(0,0,14, 5.52956327, 5.5970771));
        ls.add(new KeyedFeature(0,0,15, 5.6065421,  5.6181283));

        EventDector ed = new EventDector();
        ed.updateClustering(ls);
        System.out.println(ed.getClusteringStructure());
    }
}
