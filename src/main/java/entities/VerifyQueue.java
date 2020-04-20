package entities;

import utils.Config;

import java.io.Serializable;
import java.security.Key;
import java.util.*;

public class VerifyQueue implements Serializable {
    private long startIdx;
    private final long MAX_SIZE = Config.w2_size*12;
    private Deque<KeyedFeature> buffered;

    public VerifyQueue() {
        startIdx = 0;
        buffered = new ArrayDeque<KeyedFeature>();
    }

    public void addElement(KeyedFeature point) {
         if (buffered.size() > MAX_SIZE + 1) {
//             buffered.clear();
             buffered.removeFirst();
             startIdx += 1;
             buffered.addLast(point);
         } else {
             buffered.addLast(point);
         }
    }
    public void clear() {
        buffered.clear();
    }

    public int size() {
        return buffered.size();
    }

    public List<KeyedFeature> getElements() {
        List<KeyedFeature> ls = new ArrayList<>();
        Iterator<KeyedFeature> it = buffered.iterator();
        while(it.hasNext()) {
            ls.add(it.next());
        }
        return ls;
    }
    public List<KeyedFeature> subWindow(long start, long end) {
        return getElements().subList((int)(start-this.startIdx), (int)(end-this.startIdx));
    }

//    public List<KeyedFeature> subWindowToSecondLast(int start) {
//        return getElements().subList(start-this.startIdx, getElements().size()-1);
//    }
}
