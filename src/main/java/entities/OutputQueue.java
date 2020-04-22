package entities;

import java.io.Serializable;
import java.util.*;

public class OutputQueue implements Serializable {
    public Map<Long, DetectedEvent> queue = new HashMap<>();
    public long currentStart = 0;

    public void addElement(DetectedEvent evt){
        queue.put(evt.getBatchCounter(), evt);
    }

    public List<DetectedEvent> outputToBound(Long currentOutputBound){
        List<DetectedEvent> ls = new ArrayList<>();
        List<Long> temp = new ArrayList<>();
        temp.addAll(queue.keySet());
        for (Long idx: temp){
            if (idx < currentOutputBound){
                ls.add(queue.get(idx));
                queue.remove(idx);
            }
        }
        currentStart = currentOutputBound;
        return ls;
    }

    public Collection<DetectedEvent> outputAll(){
        List<DetectedEvent> ls = new ArrayList<>();
        ls.addAll(queue.values());
        queue.clear();
        return ls;
    }

    public void update(Long idx, DetectedEvent evt){
        queue.put(idx, evt);
    }

}
