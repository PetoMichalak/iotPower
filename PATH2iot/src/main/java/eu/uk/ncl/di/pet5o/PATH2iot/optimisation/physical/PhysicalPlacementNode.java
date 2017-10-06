package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical;

import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;

import java.util.ArrayList;

/**
 * Created by peto on 09/03/2017.
 *
 * Holds placement information regarding an InfrastructureNodes:
 * * operators
 * * outgoing edges
 */
public class PhysicalPlacementNode {
    private InfrastructureNode thisNode;
    private ArrayList<CompOperator> ops;
    private ArrayList<Integer> outgoingEdges;

    public PhysicalPlacementNode(InfrastructureNode thisNode) {
        this.thisNode = thisNode;
        ops = new ArrayList<>();
        outgoingEdges = new ArrayList<>();
    }

    public PhysicalPlacementNode() {
        thisNode = null;
        ops = new ArrayList<>();
        outgoingEdges = new ArrayList<>();
    }

    public void addOperator(CompOperator op) {
        ops.add(op);
    }

    public void addOps(ArrayList<CompOperator> ops) {
        this.ops.addAll(ops);
    }

    public int getOpCount() {
        return ops.size();
    }

    public void addOutgoingEdge(int nodeId) {
        outgoingEdges.add(nodeId);
    }

    public ArrayList<CompOperator> getOps() {
        return ops;
    }

    public void setOps(ArrayList<CompOperator> ops) {
        this.ops = ops;
    }

    public ArrayList<Integer> getOutgoingEdges() {
        return outgoingEdges;
    }

    public void setOutgoingEdges(ArrayList<Integer> outgoingEdges) {
        this.outgoingEdges = outgoingEdges;
    }

    public InfrastructureNode getThisNode() {
        return thisNode;
    }

    public void setThisNode(InfrastructureNode thisNode) {
        this.thisNode = thisNode;
    }


    @Override
    public String toString() {
        String outOps = "";
        for (CompOperator op : ops) {
            outOps += op.getNodeId() + ",";
        }

        String outEdges = "";
        for (Integer edge : outgoingEdges) {
            outEdges += edge + ",";
        }

        return String.format("%d[%s]->[%s]", thisNode.getNodeId(), outOps, outEdges);
    }
}
