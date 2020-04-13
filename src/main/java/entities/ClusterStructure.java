package entities;

import java.util.List;

public class ClusterStructure {
    public List<Integer> idxList;
    public int u;
    public int v;
    public double loc;

    public ClusterStructure(List<Integer> idxList, int u, int v, double loc) {
        this.idxList = idxList;
        this.u = u;
        this.v = v;
        this.loc = loc;
    }

    @Override
    public String toString() {
        return "ClusterStructure{" +
                "idxList=" + idxList +
                ", u=" + u +
                ", v=" + v +
                ", loc=" + loc +
                '}';
    }
}
