package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical;

import eu.uk.ncl.di.pet5o.PATH2iot.input.udfs.UdfDefs;
import eu.uk.ncl.di.pet5o.PATH2iot.input.udfs.UdfEntry;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.NeoHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;

/**
 * A linked list of operators representing logical plan.
 *
 * @author Peter Michalak
 */
public class LogicalPlan {

    private static Logger logger = LogManager.getLogger(LogicalPlan.class);

    private NeoHandler neoHandler;
    private UdfDefs udfs;
    private int nodeCount;
    private ArrayList<CompOperator> operators;
    private ArrayList<CompOperator> firstOperators;

    /**
     * Standard constructor pulling the data from neo4j db.
     */
    public LogicalPlan(NeoHandler neoHandler, UdfDefs udfs) {
        this.neoHandler = neoHandler;
        this.udfs = udfs;

        // build a graph of computation
        nodeCount = buildGraph();

        // find the beginning - definition - no incoming nodes
        firstOperators = getFirstOperators();
        logger.debug(String.format("A logical plan was build with %d nodes. First operators: ", nodeCount));
        for (CompOperator op : firstOperators) {
            logger.debug("- " + op);
        }
    }

    /**
     * Constructor used when deep copying the plan.
     */
    public LogicalPlan(UdfDefs udfs) {
        this.udfs = udfs;
        this.operators = new ArrayList<>();
    }

    public LogicalPlan() {
        operators = new ArrayList<>();
    }

    public int buildGraph() {
        int nodeCount = 0;
        // get all operators
        ArrayList<Integer> compIds = neoHandler.getAllVertices("COMP");

        // get all operators
        operators = new ArrayList<>();
        for (Integer nodeId : compIds) {
            CompOperator op = neoHandler.getOperatorById(nodeId);
            op.setDownstreamOpIds(neoHandler.findDownstreamNodes(op.getNodeId(), "COMP", "COMP",
                    "DOWNSTREAM", 1));
//            op.setDataOut((double) neoHandler.getNodeProperty2(op.getNodeId(), "COMP", "dataOut"));
            operators.add(op);
            nodeCount++;
        }

        return nodeCount;
    }

    public ArrayList<CompOperator> getFirstOperators() {
        ArrayList<CompOperator> firstOps = new ArrayList<>();
        for (CompOperator op : operators) {
            boolean isFirst = true;
            for (CompOperator tempOp : operators) {
                if (tempOp.getDownstreamOpIds().contains(op.getNodeId())) {
                    isFirst = false;
                }
            }
            if (isFirst) {
                firstOps.add(op);
            }
        }
        return firstOps;
    }

    public void displayGraph() {
        // ToDo lookup https://github.com/nidi3/graphviz-java
        logger.error("GraphViz integration is not supported yet.");
    }

    public CompOperator getOperator(Integer downstreamOpId) {
        for (CompOperator operator : operators) {
            if (operator.getNodeId() == downstreamOpId)
                return operator;
        }
        return null;
    }

    /**
     * Scans UDF definitions and Computation Catalogue to find a cost for the operator.
     *
     * @param op operator
     * @return execution cost of the operator
     */
    public double getOperatorCost(CompOperator op) {
        // if UDF - cost from definition
        switch (op.getType()) {
            case "UDF":
                for (UdfEntry udf : udfs.getUdf()) {
                    if (udf.getName().equals(op.getName())) {
                        return udf.getSupport().get(0).getMetrics().get(0).getCpuCost();
                    }
                }
                break;
            case "FilterStream":
                return 1;
            case "CountProjectionExpression":
                return 1;
        }
        logger.error(String.format("Operator type %s doesn't have a cost associated with it!", op.getType()));
        return 0;
    }

    /**
     * Finds maximum nodeId and returns an increment
     */
    public int getNewId() {
        int maxId = 0;
        for (CompOperator operator : operators) {
            if (maxId < operator.getNodeId())
                maxId = operator.getNodeId();
        }
        return maxId + 1;
    }

    @Override
    public String toString() {
        String out = String.format("LogPlan: %s (%d nodes): ", this.hashCode(), operators.size());
        for (CompOperator operator : operators) {
            out += String.format("%d(%s)->%s | ", operator.getNodeId(), operator.getType(),
                    operator.getDownstreamOpIds());
        }
        return out;
    }

    /**
     * Scans the plan and checks whether there are windows present
     **/
    public boolean hasWindows() {
        for (CompOperator operator : operators) {
            if (operator.getType().equals("win")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Scans the plan and return the operator (optional skipping e.g. get 3rd window).
     */
    public CompOperator getOperator(int skip, String type) {
        int skipCounter = 0;
        for (CompOperator op : operators) {
            if (op.getType().equals(type)) {
                if (skipCounter == skip) {
                    return op;
                } else {
                    skipCounter++;
                }
            }
        }
        return null;
    }

    /**
     * Removes the given operator from current position within the logical plan and
     * moves it closer to the data source (upstream).
     * Returns a deep copy of the original plan.
     */
    public LogicalPlan pushOperatorUpstream(CompOperator op) {
        // check whether the operator belongs to this plan
        if (! operators.contains(op)) {
            return null;
        }

        // copy the current plan
        LogicalPlan nextPlan = getCopy();

        // get the window operator
        CompOperator winOp = nextPlan.getOperator(op.getNodeId());

        // get grandparent of the operator (2 hops upstream)
        CompOperator parent = nextPlan.findUpstreamOp(op);
        CompOperator grandParent = nextPlan.findUpstreamOp(parent);

        // if there is not grandparent - don't push windows
        if (grandParent == null) {
            return null;
        }

        // remove the operator from the plan
        nextPlan.remove(winOp);

        // link the operator in between the grandparent and parent
        nextPlan.insert(winOp, grandParent, parent);

        return nextPlan;
    }

    /**
     * Scans the plan and returns a parent (upstream operator) of a given op.
     * Null if not found, or given operator at the head.
     */
    private CompOperator findUpstreamOp(CompOperator op) {
        for (CompOperator operator : operators) {
            for (Integer downstreamNodeId : operator.getDownstreamOpIds()) {
                if (downstreamNodeId == op.getNodeId()) {
                    logger.debug(String.format("Parent found: %d streams to %d.",
                            operator.getNodeId(), op.getNodeId()));
                    return operator;
                }
            }
        }
        // no match in downstream node id
        return null;
    }

    /**
     * Returns a deep copy the logical plan.
     */
    public LogicalPlan getCopy() {
        LogicalPlan planCopy = new LogicalPlan(udfs);
        for (CompOperator operator : operators) {
            CompOperator opCopy = operator.getCopy();
            planCopy.addOperator(opCopy);
        }
        return planCopy;
    }

    /**
     * Adds an operator to the list of operators.
     */
    public void addOperator(CompOperator op) {
        operators.add(op);
    }

    /**
     * Finds parent, and relinks any streams to downstream node, before deleting itself from the plan.
     */
    private void remove(CompOperator op) {
        // find parent
        CompOperator parent = findUpstreamOp(op);

        // add downstream links to the parent
        for (Integer downNodeId : op.getDownstreamOpIds()) {
            parent.addDownstreamOp(downNodeId);
        }
        op.clearDownstreamOps();

        // remove link from the parent to the op
        parent.removeDownstreamOp(op.getNodeId());

        // remove the operator from the plan
        this.removeOpById(op.getNodeId());
    }

    /**
     * Scans the plan and removes the operator from it.
     */
    private void removeOpById(int nodeId) {
        CompOperator opToRemove = null;
        for (CompOperator operator : operators) {
            if (operator.getNodeId() == nodeId) {
                opToRemove = operator;
                break;
            }
        }
        operators.remove(opToRemove);
    }


    /**
     * Inserts an operator in between the two provided operators. Relinks all the downstreaming.
     */
    private void insert(CompOperator op, CompOperator firstOp, CompOperator thirdOp) {
        // remove pointer from first to third
        firstOp.removeDownstreamOp(thirdOp.getNodeId());

        // add pointer op -> third
        op.addDownstreamOp(thirdOp.getNodeId());

        // add pointer from first to the new one
        firstOp.addDownstreamOp(op.getNodeId());

        // insert node to the list
        addOperator(op);
    }


    /**
     * @return number of operators within the plan
     */
    public int getOpCount() {
        return operators.size();
    }

    public ArrayList<CompOperator> getOperators() {
        return operators;
    }
}