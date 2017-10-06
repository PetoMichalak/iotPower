package eu.uk.ncl.di.pet5o.PATH2iot.operator;

import java.util.ArrayList;

/**
 * Created by peto on 27/02/2017.
 * Placeholder to persist the placement information
 */
public class CompPlacement {
    private int compId;
    private ArrayList<Integer> nodeIds;

    public CompPlacement(int compId, ArrayList<Integer> nodeIds) {
        this.compId = compId;
        this.nodeIds = nodeIds;
    }

    public CompPlacement(int compId, int nodeId) {
        this.compId = compId;
        this.nodeIds = new ArrayList<>();
        nodeIds.add(nodeId);
    }

    public int getCompId() {
        return compId;
    }

    public void setCompId(int compId) {
        this.compId = compId;
    }

    public ArrayList<Integer> getNodeIds() {
        return nodeIds;
    }

    public void setNodeIds(ArrayList<Integer> nodeIds) {
        this.nodeIds = nodeIds;
    }

    @Override
    public String toString() {
        return String.format("{compId: %d on %s}", compId, nodeIds);
    }
}
