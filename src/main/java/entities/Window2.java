package entities;


import java.util.*;

public class Window2 {
    private final int MAX_SIZE = 100;
    private Deque<Feature> w2 = new ArrayDeque<Feature>();

    public void addElement(Feature point){
        if (w2.size() == MAX_SIZE){
            w2.removeFirst();
            w2.addLast(point);
        }
        else{
            w2.addLast(point);
        }
    }


    public int size(){
        return w2.size();
    }

    public List<Feature> getElements(){
        List<Feature> ls = new ArrayList<>();
        Iterator<Feature> it = w2.iterator();
        while(it.hasNext()){
            ls.add(it.next());
        }
        return ls;
    }

}
