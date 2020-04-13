package entities;

import java.util.*;

import utils.Config;

public class Window2 {
    private final int MAX_SIZE = Config.w2_size;
    private Deque<KeyedFeature> w2 = new ArrayDeque<KeyedFeature>();

    public Window2() {}
    
    public void addElement(KeyedFeature point) {
        // if (w2.size() > MAX_SIZE + 1) {
        //     w2.clear();
        //     w2.removeFirst();
        //     w2.addLast(point);
        // } else {
        //     w2.addLast(point);
        // }

        w2.addLast(point);
    }

    public void removeFirst() {
        w2.removeFirst();
    }

    public void clear() {
        w2.clear();
    }

    public int size() {
        return w2.size();
    }

    public void setW2(List<KeyedFeature> features) {
        w2.clear();
        w2.addAll(features);
    } 

    public List<KeyedFeature> subWindow(int start, int end) {
        return getElements().subList(start, end);
    }

    public List<KeyedFeature> getElements() {
        List<KeyedFeature> ls = new ArrayList<>();
        Iterator<KeyedFeature> it = w2.iterator();
        while(it.hasNext()) {
            ls.add(it.next());
        }
        return ls;
    }

    @Override
    public String toString() {
        return "Window2： " + w2;
    }
}
