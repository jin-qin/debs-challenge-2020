package entities;

import java.io.Serializable;
import java.util.*;

import utils.Config;

public class Window2 implements Serializable{
    private final int MAX_SIZE = Config.w2_size;
    private Deque<KeyedFeature> w2 = new ArrayDeque<KeyedFeature>();
    private Deque<Feature> w2nokey = new ArrayDeque<Feature>();
    private Boolean isNoKey = false;

    public Window2() {}
    public Window2(Boolean nokey) { isNoKey = nokey; }

    public void setIsNoKey(Boolean nokey) {
        isNoKey = nokey;
    }
    
    public <T> void addElement(T point) {
        // if (w2.size() > MAX_SIZE + 1) {
        //     w2.clear();
        //     w2.removeFirst();
        //     w2.addLast(point);
        // } else {
        //     w2.addLast(point);
        // }
        
        if (isNoKey) {
            w2nokey.addLast((Feature)point);
        } else {
            w2.addLast((KeyedFeature)point);
        }
    }

    public void removeFirst() {
        if (isNoKey) {
            w2nokey.removeFirst();
        } else {
            w2.removeFirst();
        }
        
    }

    public void clear() {
        if (isNoKey) {
            w2nokey.clear();
        } else {
            w2.clear();
        }
    }

    public int size() {
        if (isNoKey) {
            return w2nokey.size();
        } else {
            return w2.size();
        }
        
    }

    public <T> void setW2(List<T> features) {
        if (isNoKey) {
            w2nokey.clear();
            w2nokey.addAll((List<Feature>)features);
        } else {
            w2.clear();
            w2.addAll((List<KeyedFeature>) features);
        }
        
    }

    public <T> List<T> subWindow(int start, int end) {
        return (List<T>) getElements().subList(start, end);
    }

    public <T> List<T> getElements() {
        if (isNoKey) {
            List<Feature> ls = new ArrayList<>();
            Iterator<Feature> it = w2nokey.iterator();
            while(it.hasNext()) {
                ls.add(it.next());
            }
            return (List<T>) ls;
        } else {
            List<KeyedFeature> ls = new ArrayList<>();
            Iterator<KeyedFeature> it = w2.iterator();
            while(it.hasNext()) {
                ls.add(it.next());
            }
            return (List<T>) ls;
        }
    }

    @Override
    public String toString() {
        return "Window2： " + w2;
    }
}
