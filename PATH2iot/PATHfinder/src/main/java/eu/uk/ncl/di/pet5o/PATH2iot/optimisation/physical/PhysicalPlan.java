package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical;

import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.input.udfs.UdfDefs;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A physical mapping of infrastructure to the operators:
 * * HashMap<node, [operators]>
 *
 * @author Peter Michalak
 */
public class PhysicalPlan {

    private static Logger logger = LogManager.getLogger(PhysicalPlan.class);

    private Map<InfrastructureNode, ArrayList<CompOperator>> placement;
    private ArrayList<CompOperator> currentOps;
    // a placeholder for a transfer operator id
    private int nextSxferOpId = 999000;
    private double energyCost;

    public PhysicalPlan() {
        placement = new HashMap<>();
        currentOps = new ArrayList<>();
    }

    /**
     * Adds supplied operator on the node.
     */
    public void place(CompOperator op, InfrastructureNode node) {
        if (placement.containsKey(node)) {
            placement.get(node).add(op);
            currentOps.add(op);
        } else {
            ArrayList<CompOperator> temp = new ArrayList<>();
            temp.add(op);
            placement.put(node, temp);
            currentOps.add(op);
        }
    }

    /**
     * @return list of all operators
     */
    public ArrayList<CompOperator> getCurrentOps() {
        return currentOps;
    }

    /**
     * Makes a deep copy of the physical plan
     * @return new physical plan (copy of the current instance)
     */
    public PhysicalPlan getCopy() {
       PhysicalPlan ppCopy = new PhysicalPlan();
        for (InfrastructureNode node : placement.keySet()) {
            for (CompOperator operator : placement.get(node)) {
                ppCopy.place(operator.getCopy(), node);
            }
        }
        return ppCopy;
    }

    /**
     * Returns an infrastructure node which has the current operator placed on it.
     */
    public InfrastructureNode getOpPlacementNode(CompOperator currentOp) {
        for (InfrastructureNode node : placement.keySet()) {
            if (placement.get(node).contains(currentOp)) {
                return node;
            }
        }
        return null;
    }

    /**
     * Returns an infrastructure node which has the supplied operator (by id) placed on it.
     */
    private InfrastructureNode getOpPlacementNode(Integer opId) {
        for (InfrastructureNode node : placement.keySet()) {
            for (CompOperator operator : placement.get(node)) {
                if (operator.getNodeId() == opId) {
                    return node;
                }
            }
        }
        return null;
    }

    public Map<InfrastructureNode, ArrayList<CompOperator>> getPlacement() {
        return placement;
    }

    /**
     * Checks whether this is the last operator on the processing element AND there is a recipient of output data.
     */
    public boolean isOutgoingOperator(CompOperator op) {
        // does the operator have outgoing edges
        if (op.getDownstreamOpIds().size() == 0)
            return false;

        // is any of the downstream operators on a different node
        InfrastructureNode operatorPlacement = getOpPlacementNode(op);
        for (Integer opId : op.getDownstreamOpIds()) {
            // if the placement of downstream operator is not the same as current -> this operator is outgoing
            if (operatorPlacement != getOpPlacementNode(opId)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Determines whether two operators are coplaced.
     */
    public boolean placedTogether(CompOperator op, CompOperator downstreamOp) {
        return getOpPlacementNode(op).equals(getOpPlacementNode(downstreamOp));
    }

    /**
     * Returns an operator by specified id
     */
    public CompOperator getOp(int nodeId) {
        for (InfrastructureNode node : placement.keySet()) {
            for (CompOperator operator : placement.get(node)) {
                if (operator.getNodeId() == nodeId)
                    return operator;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        String out = "PhysPlan:";
        for (InfrastructureNode node : placement.keySet()) {
            out += String.format(" (%s%d)-[", node.getResourceType().substring(0,3), node.getNodeId());
            for (CompOperator op : placement.get(node)) {
                out += String.format("%d:%s, ", op.getNodeId(), op.getName().substring(0,5));
            }
            out += "]";
        }
        return out;
    }

    /**
     * Identifies all 'source' operators
     * - source operator is an operator that is generating the data - no other operator is streaming data into it
     * @return a collection of source operators
     */
    public ArrayList<CompOperator> getSourceOps() {
        ArrayList<CompOperator> sourceOps = new ArrayList<>();

        // loop through all nodes and keep all op that others don't point to them
        for (CompOperator op : currentOps) {
            boolean isSource = true;
            for (CompOperator tempOp : currentOps) {
                if (tempOp.getDownstreamOpIds().contains(op.getNodeId())) {
                    isSource = false;
                    break;
                }
            }
            if (isSource)
                sourceOps.add(op);
        }

        return sourceOps;
    }

    /**
     * Returns all infrastructure nodes used in this physical plan.
     */
    public Set<InfrastructureNode> getAllNodes() {
        ArrayList<InfrastructureNode> nodes = new ArrayList<>();
        return placement.keySet();
    }

    /**
     * Identifies all nodes that are at the edge, defined as:
     * - there exists at least one downstream operator
     * - the downstream operator is on a different resource
     */
    public ArrayList<CompOperator> getEdgeNodes() {
        ArrayList<CompOperator> edgeOps = new ArrayList<>();
        // scan all the nodes
        for (CompOperator op : currentOps) {
            InfrastructureNode opNode = getOpPlacementNode(op);
            // is the downstream operator on a different node
            for (Integer downstreamOpId : op.getDownstreamOpIds()) {
                InfrastructureNode downstreamNode = getOpPlacementNode(downstreamOpId);
                if (downstreamNode != opNode) {
                    // the operator and the downstream operator are on different platforms / resources
                    edgeOps.add(op);
                }
            }

        }
        return edgeOps;
    }

    /**
     * Keep track of the sxfer operator ids.
     */
    public int getNextSxferOpId() {
        return ++nextSxferOpId;
    }

    /**
     * @return a nex sxfer operator
     */
    public CompOperator createSxferOp() {
        CompOperator sxfer = new CompOperator(getNextSxferOpId());
        sxfer.setName("sxfer");
        sxfer.setType("sxfer");
        sxfer.setOperator("forward");
        sxfer.setSelectivityRatio(1);
        sxfer.setGenerationRatio(1);
        return sxfer;
    }

    public double getEnergyCost() {
        return energyCost;
    }

    public void setEnergyCost(double energyCost) {
        this.energyCost = energyCost;
    }
}