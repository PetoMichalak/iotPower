package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical;

import eu.uk.ncl.di.pet5o.PATH2iot.infrastructure.InfrastructurePlan;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical.LogicalPlan;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.*;

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
    private LogicalPlan logPlan;
    // a placeholder for a transfer operator id
    private int nextSxferOpId = 999000;
    private double energyCost;

    public PhysicalPlan(LogicalPlan logPlan) {
        this.logPlan = logPlan;
        placement = new HashMap<>();
        currentOps = new ArrayList<>();
    }

    /**
     * Adds supplied operator on the node.
     */
    public void place(CompOperator op, InfrastructureNode node, InfrastructurePlan infra) {
        // find the operator from this instance of physical plan
        logger.debug(String.format("Placing %d on %d", op.getNodeId(), node.getNodeId()));
        op = logPlan.getOperator(op.getNodeId());

        // inject sxfer if parent op doesn't have a direct link to this operator
        CompOperator parentOp = getParentOp(op);
        if (parentOp != null)
            addSxfer(op, node, parentOp, infra);

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
     * Checks whether sxfer is needed (if nodes are not neighbours);
     * adds sxfer if needed
     */
    private void addSxfer(CompOperator op, InfrastructureNode node, CompOperator parentOp, InfrastructurePlan infra) {
        // get the parent node
        InfrastructureNode parentNode = getOpPlacementNode(parentOp);

        if (!(parentNode.getDownstreamNodes().contains(node.getNodeId())) && (parentNode != node)) {
            // the nodes are not neighbours - add sxfer
            CompOperator sxferOp = createSxferOp();

            // make sxfer to stream to the op
            sxferOp.addDownstreamOp(op.getNodeId());

            // make parent operator stream data to sxfer
            parentOp.replaceDownstreamOp(op.getNodeId(), sxferOp.getNodeId());

            // place sxfer on the intermediate node
            int intermediateNodeId = parentNode.getDownstreamNodes().get(0);
            place(sxferOp, infra.getNodeById(intermediateNodeId), infra);
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
        PhysicalPlan ppCopy = new PhysicalPlan(logPlan.getCopy());
        for (InfrastructureNode node : placement.keySet()) {
            for (CompOperator operator : placement.get(node)) {
                ppCopy.directPlacement(ppCopy.logPlan.getOperator(operator.getNodeId()), node);
            }
        }
        logger.debug(String.format("Cloning the physical plan %s\n%s", ppCopy, ppCopy.getLogPlan()));
        return ppCopy;
    }

    /**
     * Places operator directly on the node - used when deep copying the physical plans
     */
    private void directPlacement(CompOperator op, InfrastructureNode node) {
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
     * Returns an infrastructure node which has the current operator placed on it.
     */
    public InfrastructureNode getOpPlacementNode(CompOperator currentOp) {
        for (InfrastructureNode node : placement.keySet()) {
            for (CompOperator tempOp : placement.get(node)) {
                if (tempOp.getNodeId()==currentOp.getNodeId())
                    return node;
            }
        }
        return null;
    }

    /**
     * Returns an infrastructure node which h as the supplied operator (by id) placed on it.
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
     * Creates a sxfer transfer operator with default values
     *
     * @return a nex sxfer operator
     */
    public CompOperator createSxferOp() {
        CompOperator sxfer = new CompOperator(getNextSxferOpId());
        sxfer.setName("sxfer");
        sxfer.setType("sxfer");
        sxfer.setOperator("forward");
        sxfer.setSelectivityRatio(1);
        sxfer.setGenerationRatio(1);

        // add it to the logical plan
        logPlan.addOperator(sxfer);

        return sxfer;
    }

    public double getEnergyCost() {
        return energyCost;
    }

    public void setEnergyCost(double energyCost) {
        this.energyCost = energyCost;
    }

    /**
     * Calculated the estimated battery life under current plan.
     * @return Estimated battery life in hours.
     */
    public double getEstimatedLifetime(InfrastructureDesc infra, String device) {
        // calculate the battery power capacity (capacity x voltage x 3.6)
        double powerCapacity = 0;
        for (InfrastructureNode node : infra.getNodes()) {
            if (node.getResourceType().equals(device)) {
                powerCapacity = node.getBatteryCapacity_mAh() * node.getBatteryVoltage_V() * 3.6;
            }
        }

        // calculate the estimated battery life (s)
        // energy cost is calculated in mJ, therefore '/ 1000'
        double estimatedBatteryLife = powerCapacity / (energyCost / 1000);

        // recalculate for hours
        return estimatedBatteryLife / 60 / 60;
    }

    /**
     * @return arraylist of operators placed on the node
     */
    public ArrayList<CompOperator> getOpsOnNode(InfrastructureNode node) {
        return placement.get(node);
    }

    public LogicalPlan getLogPlan() {
        return logPlan;
    }

    /**
     * Validates whether all operators have been placed.
     */
    public boolean isComplete() {
        // check that all operators have been placed
        for (CompOperator op : logPlan.getOperators()) {
            if (!isPlaced(op)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks whether given operator is placed in the current physical plan.
     */
    private boolean isPlaced(CompOperator op) {
        for (InfrastructureNode node : placement.keySet()) {
            for (CompOperator compOperator : placement.get(node)) {
                if (compOperator.getNodeId() == op.getNodeId()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Loops from the beginning of the chain of operators and returns first unplaced operator.
     * @return
     */
    public CompOperator getUnplacedOperators() {
        CompOperator unplacedOp = null;
        for (CompOperator compOperator : logPlan.getFirstOperators()) {
            if (!isPlaced(compOperator))
            {
                unplacedOp = compOperator;
                break;
            }
            else
            {
                unplacedOp = getNextUnplacedOperator(compOperator);
                if (unplacedOp != null) {
                    break;
                }
            }
        }
        logger.debug(String.format("Unplaced operator found: %s: %s\n(%s)",
                unplacedOp, this, logPlan));
        return unplacedOp;
    }

    /**
     * finds next unplaced operator starting from the operator provided.
     */
    private CompOperator getNextUnplacedOperator(CompOperator compOperator) {
        CompOperator unplacedOp = null;
        for (Integer opId : compOperator.getDownstreamOpIds()) {
            CompOperator tempOp = logPlan.getOperator(opId);
            if (!isPlaced(tempOp)) {
                return tempOp;
            } else {
                unplacedOp = getNextUnplacedOperator(tempOp);
                if (unplacedOp != null) {
                    return unplacedOp;
                }
            }
        }
        return unplacedOp;
    }

    /**
     * Finds the parent op and returns a list of all nodes (including the parent node).
     */
    public List<InfrastructureNode> getPlacementNodes(CompOperator op, InfrastructurePlan infra) {
        List<InfrastructureNode> nodes = new ArrayList<>();

        // find parent operator
        CompOperator parentOp = getParentOp(op);

        // get infrastructure node
        InfrastructureNode parentNode = getOpPlacementNode(parentOp);
        nodes.add(parentNode);

        // get all downstream infrastructure nodes
        infra.getDownstreamNodes(parentNode, nodes);

        return nodes;
    }

    /**
     * loops through all operators and returns the parent node
     */
    private CompOperator getParentOp(CompOperator op) {
        for (CompOperator compOperator : logPlan.getOperators()) {
            if (compOperator.getDownstreamOpIds().contains(op.getNodeId())) {
                return compOperator;
            }
        }
        return null;
    }

    /**
     * Scans the plan and returns True/False based on the positioning of this operator.
     */
    public boolean isSource(CompOperator op) {
        // scan all operators, if one points to the 'op', 'op' is not a source
        for (CompOperator tempOp : currentOps) {
            if (tempOp.getDownstreamOpIds().contains(op.getNodeId())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Return downstream operators for this operator on the same physical node.
     */
    public ArrayList<CompOperator> getDownNodeOps(CompOperator op) {
        // get the physical node
        InfrastructureNode node = getOpPlacementNode(op);

        // get all operators on the physical node
        ArrayList<CompOperator> opsOnNode = getOpsOnNode(node);

        // collect all downstream operators from 'op'
        ArrayList<CompOperator> downOps = new ArrayList<>();
        for (Integer downOpId : op.getDownstreamOpIds()) {
            // check that the operator is placed on the node
            CompOperator downOp = logPlan.getOperator(downOpId);
            if (opsOnNode.contains(downOp)) {
                downOps.add(downOp);
            }
        }
        return downOps;
    }
}