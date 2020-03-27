package entities;

import java.util.*;

public class Window2 {
    private final int MAX_SIZE = 100;
    private Deque<KeyedFeature> w2 = new ArrayDeque<KeyedFeature>();

    public Window2() {}
    
    public void addElement(KeyedFeature point) {
        if (w2.size() == MAX_SIZE) {
            w2.removeFirst();
            w2.addLast(point);
        } else {
            w2.addLast(point);
        }
    }


    public int size() {
        return w2.size();
    }

    public List<KeyedFeature> getElements() {
        List<KeyedFeature> ls = new ArrayList<>();
        Iterator<KeyedFeature> it = w2.iterator();
        while(it.hasNext()) {
            ls.add(it.next());
        }
        return ls;
    }
}
