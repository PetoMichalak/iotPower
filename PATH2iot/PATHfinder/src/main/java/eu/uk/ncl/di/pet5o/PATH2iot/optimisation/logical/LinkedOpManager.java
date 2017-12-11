package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical;

import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.NodeCapability;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.NeoHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by peto on 28/05/2017.
 *
 * Serves as a controller for LinkedOperator services.
 */
public class LinkedOpManager {

    private static Logger logger = LogManager.getLogger(LinkedOpManager.class);

    public LinkedOpManager() { }

    /**
     * Creates the list, operator and makes first placement.
     */
    public List<LinkedOperator> initialPlacement(int opId, int nodeId) {
        List<LinkedOperator> linkedOperators = new ArrayList<>();
        LinkedOperator op = new LinkedOperator(opId);
        op.setPlacement(nodeId);
        linkedOperators.add(op);
        return linkedOperators;
    }

    /**
     * Verifies currently placed operators and returns next operators to be placed
     */
    public List<Integer> getNextUnplacedOp(LinkedOperator op, List<LinkedOperator> plan, LogicalPlan logPlan) {
        List<Integer> nextOps = new ArrayList<>();
        // get downstream operators for this operator
        CompOperator operator = logPlan.getOperator(op.getOpId());
        if (operator == null) {
            return  nextOps;
        }
        for (Integer downstreamOpId : operator.getDownstreamOpIds()) {
            // if not placed in this plan yet: add it
            if (getPlacementNodeId(downstreamOpId, plan) == -1) {
                nextOps.add(downstreamOpId);
            }
        }

        return nextOps;
    }

    /**
     * Returns the node id of the node where this operator is placed, if not placed returns -1.
     */
    public int getPlacementNodeId(Integer opId, List<LinkedOperator> plan) {
        int nodeId = -1;
        // scans all plan
        for (LinkedOperator linkedOperator : plan) {
            if (linkedOperator.getOpId() == opId) {
                nodeId = linkedOperator.getPlacementNodeIds().get(0);
            }
        }
        return nodeId;
    }

    /**
     * Creates a copy of plan and places new operator.
     */
    public List<LinkedOperator> placeOp(List<LinkedOperator> origPlan, Integer opId, int nodeId) {
        List<LinkedOperator> linkedOperators = getPlanCopy(origPlan);
        LinkedOperator op = new LinkedOperator(opId);
        op.setPlacement(nodeId);
        linkedOperators.add(op);
        return linkedOperators;
    }

    /**
     * Returns a new copy of the passed plan.
     */
    private List<LinkedOperator> getPlanCopy(List<LinkedOperator> origPlan) {
        List<LinkedOperator> linkedOperators = new ArrayList<>();
        for (LinkedOperator linkedOperator : origPlan) {
            LinkedOperator tempOp = new LinkedOperator(linkedOperator.getOpId());
            // copy all downstream ops
            for (Integer downstreamOpId : linkedOperator.getDownstreamOpIds()) {
                tempOp.setDownstreamOp(downstreamOpId);
            }
            // copy all placements
            for (Integer nodeId : linkedOperator.getPlacementNodeIds()) {
                tempOp.setPlacement(nodeId);
            }
            linkedOperators.add(tempOp);
        }
        return linkedOperators;
    }

    /**
     * Creates new link between operator and downstream op.
     */
    public void linkNodes(int opId, int nextOp, List<LinkedOperator> nextPlan) {
        for (LinkedOperator linkedOperator : nextPlan) {
            if (linkedOperator.getOpId() == opId) {
                linkedOperator.setDownstreamOp(nextOp);
                break;
            }
        }
    }

    public int getNewOpId(List<LinkedOperator> plan) {
        int newId = 999000;
        for (LinkedOperator linkedOperator : plan) {
            if (linkedOperator.getOpId() > newId) {
                newId = linkedOperator.getOpId();
            }
        }
        return newId + 1;
    }

    /**
     * Format the plan and print it out.
     */
    public String printPlan(List<LinkedOperator> pPlan) {
        String out = "";
        for (LinkedOperator linkedOperator : pPlan) {
            out += String.format("%d %s; ", linkedOperator.getOpId(), linkedOperator.getPlacementNodeIds());
        }
        return out;
    }

    /**
     * Verifies that all operations can be performed on nodes according to the plan.
     */
    public boolean isDeployable(List<LinkedOperator> physicalPlan, InfrastructureDesc infra,
                                NeoHandler neoHandler) {
        boolean canBeDeployed = true;
        // for each operator check that it can be run on the given platform
        for (LinkedOperator linkedOp : physicalPlan) {
            CompOperator compOp = neoHandler.getOperatorById(linkedOp.getOpId());

            if (compOp.isShell()) { // handle special sxfer operator
                compOp.setType("sxfer");
                compOp.setOperator("forward");
            }

            // check that operation can be performed on given platform
            for (Integer infraNodeId : linkedOp.getPlacementNodeIds()) {
                InfrastructureNode infraNode = infra.getNodeById(infraNodeId);
                if (! canRun(infraNode.getCapabilities(), compOp.getRequirements())) {
                    logger.debug("Deployment requirement not met - pruning! Operator: " +
                            linkedOp.getOpId() + "; InfraNode: " + infraNode.getNodeId());
                    return false;
                }
            }
        }
        return canBeDeployed;
    }

    /**
     * Compares the requirement with capabilities.
     * Wildcard is allowed in capability description (only in operator section).
     */
    private boolean canRun(List<NodeCapability> capabilities, String requirements) {
        for (NodeCapability cap : capabilities) {
            // type has to match (capability defined as <type>:<operator>
            String reqType = requirements.substring(0, requirements.indexOf(":"));
            String capType = cap.getName();

            if (capType.equals(reqType)) {
                // compare operators - consider wildcard
                String reqOp = requirements.substring(requirements.indexOf(":") + 1);
                String capOp = cap.getOperator();

                // supports all operators?
                if (capOp.equals("*")) {
                    return true;
                }

                // supports given operator
                if (capOp.equals(reqOp)) {
                    return true;
                }
            }
        }
        return false;
    }
}
