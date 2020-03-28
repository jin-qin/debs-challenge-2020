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
        ls.add(new KeyedFeature(0,0,0, 1, 2));
        ls.add(new KeyedFeature(0,1,1, 1.01, 2.02));
        ls.add(new KeyedFeature(0,2,2, 0.99, 2.03));
        ls.add(new KeyedFeature(0,3,3, 1.02, 1.97));

        ls.add(new KeyedFeature(0,4,4, 100, 300.1));
        ls.add(new KeyedFeature(0,5,5, 20.03, 4.97));
        ls.add(new KeyedFeature(0,5,5, 20.01, 4.98));

        ls.add(new KeyedFeature(0,5,5, 10.01, 5.02));
        ls.add(new KeyedFeature(0,6,6, 9.99, 4.99));
        ls.add(new KeyedFeature(0,5,5, 10.02, 5.01));
        ls.add(new KeyedFeature(0,6,6, 9.98, 4.98));

        EventDector ed = new EventDector();
        ed.updateClustering(ls);
        System.out.println(ed.getClusteringStructure());
    }
}
