package entities;


import java.util.*;

public class Window2 {
    private final int MAX_SIZE = 100;
    private Deque<Point> w2 = new ArrayDeque<Point>();

    public void addElement(Point point){
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

    public List<Point> getElements(){
        List<Point> ls = new ArrayList<>();
        Iterator<Point> it = w2.iterator();
        while(it.hasNext()){
            ls.add(it.next());
        }
        return ls;
    }

}
