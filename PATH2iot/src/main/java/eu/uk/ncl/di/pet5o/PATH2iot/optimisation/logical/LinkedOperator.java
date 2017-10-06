package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by peto on 28/05/2017.
 *
 * Representaion of a single operator entity with relationship to placement and output edges.
 */
public class LinkedOperator {

    private int opId;
    private List<Integer> downstreamOpIds;
    private List<Integer> placementNodeIds;

    public LinkedOperator(int nodeId) {
        this.opId = nodeId;
        downstreamOpIds = new ArrayList<>();
        placementNodeIds = new ArrayList<>();
    }

    public void setPlacement(int nodeId) {
        placementNodeIds.add(nodeId);
    }

    public void setDownstreamOp(int opId) {
        downstreamOpIds.add(opId);
    }

    public void removeDownstreamOp(int opId) {
        List<Integer> tempDownOpIds = new ArrayList<>();
        for (Integer downstreamOpId : downstreamOpIds) {
            if (downstreamOpId != opId) {
                tempDownOpIds.add(downstreamOpId);
            }
        }
    }

    public void removePlacement(int nodeId) {
        List<Integer> tempPlacementIds = new ArrayList<>();
        for (Integer placementNodeId : placementNodeIds) {
            if (placementNodeId != nodeId) {
                tempPlacementIds.add(placementNodeId);
            }
        }
    }

    public int getOpId() {
        return opId;
    }

    public List<Integer> getDownstreamOpIds() {
        return downstreamOpIds;
    }

    public List<Integer> getPlacementNodeIds() {
        return placementNodeIds;
    }

    @Override
    public String toString() {
        return String.format("LinkedOp %d [%s] -> (%s)", opId, placementNodeIds, downstreamOpIds);
    }
}
