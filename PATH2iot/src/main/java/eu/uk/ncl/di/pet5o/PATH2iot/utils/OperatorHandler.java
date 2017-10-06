package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import com.google.common.base.Charsets;
import com.google.common.io.CharSink;
import com.google.common.io.Files;
import com.google.gson.Gson;
import eu.uk.ncl.di.pet5o.PATH2iot.infrastructure.InfrastructurePlan;
import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.StreamResourcePair;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.input.udfs.UdfDefs;
import eu.uk.ncl.di.pet5o.PATH2iot.input.udfs.UdfEntry;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompPlacement;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical.LogicalPlan;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical.PhysicalPlacementGenerator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical.PhysicalPlacementNode;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical.PhysicalPlan;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * The core of the project starts here. Handle UDFs and operator pinning, then start use smarter metrics.
 *
 * @author Peter Michalak
 */
public class OperatorHandler {

    private static Logger logger = LogManager.getLogger(OperatorHandler.class);

    private NeoHandler neoHandler;
    private LogicalPlan logicalPlan;
    private ArrayList<LogicalPlan> enumeratedLogicalPlans;
    private ArrayList<PhysicalPlan> enumeratedPhysicalPlans;

    // number of maximum operators per node
    private final int MAX_OP_ON_NODE = 4;


    public OperatorHandler(NeoHandler neoHandler) {
        this.neoHandler = neoHandler;
        enumeratedLogicalPlans = new ArrayList<>();
        enumeratedPhysicalPlans = new ArrayList<>();
    }

    /**
     * Search from the given node Id along STREAMS connections to get vertices which were not yet placed.
     * @param compId starting point
     * @return list of vertex Ids which were not yet placed.
     */
    private ArrayList<Integer> findCompsToPlace(Integer compId) {
        return neoHandler.findShortestPath(compId, "COMP", "COMP", "STREAMS", "PLACED");
    }

    /**
     * pulls all the nodes which were not yet placed and
     * if there are any, places them
     */
    private boolean isPlacementComplete() {
        return neoHandler.getNotPlacedNodes().size() == 0;
    }

    /**
     * checks whether the node has been placed already
     */
    private boolean isPlaced(Integer nodeId, String type, String type2) {
        ArrayList<Integer> hosts = neoHandler.getAllHosts(nodeId, type, type2, "PLACED");
        return hosts.size() != 0;
    }

    /**
     * @return infrastructure node if found
     */
    private InfrastructureNode getNodeById(InfrastructureDesc infra, int resourceId) {
        for (InfrastructureNode node : infra.getNodes()) {
            if (node.getResourceId() == resourceId) {
                return node;
            }
        }
        return null;
    }

    /**
     * Saves the placement information to a file
     * @param path path to file
     */
    public void savePhysicalPlan(String path) throws IOException {
        logger.debug("Collecting information to save the physical plan...");
        // get all computations
        ArrayList<Integer> compIds = neoHandler.getAllVertices("COMP");
        logger.debug(String.format("Gathered %d: %s", compIds.size(), compIds));

        // get all placement info
        ArrayList<CompPlacement> compPlacements = new ArrayList<>();
        for (Integer compId : compIds) {
            compPlacements.add(new CompPlacement(compId,
                    neoHandler.getCompPlacement(compId, "COMP", "PLACED", "NODE")));
        }

        // save to a file
        Gson gson = new Gson();
        String ppJson = gson.toJson(compPlacements);
        File file = new File(path);
        CharSink sink = Files.asCharSink(file, Charsets.UTF_8);
        sink.write(ppJson);
        logger.debug("Physical plan representation persisted: " + ppJson);
    }

    /**
     * Rebuild a graph of operators with direct links in between the computations.
     */
    public void createDirectLinks(String rel) {
        // find the beginning of the graph
        ArrayList<Integer> opInitialNodes = neoHandler.findNodesWithoutIncomingRelationship("COMP", "STREAMS");
        logger.debug("Operator initial nodes: " + opInitialNodes);

        // find the downstream operator
        for (Integer opInitialNode : opInitialNodes) {
            findDownstreamNodesRec(rel, opInitialNode);
        }
    }

    private void findDownstreamNodesRec(String rel, Integer opInitialNode) {
        ArrayList<Integer> downstreamNodes = neoHandler.findDownstreamNodes(opInitialNode,
                "COMP", "COMP", "STREAMS", 5);
        logger.debug(String.format("Node %d has following downstream nodes: %s", opInitialNode, downstreamNodes));

        // create a direct link
        for (Integer downstreamNode : downstreamNodes) {
            neoHandler.createRel(opInitialNode, rel, downstreamNode);
            logger.debug("Find direct downstream nodes for " + downstreamNode);
            findDownstreamNodesRec(rel, downstreamNode);
        }
    }

    /**
     * User defined stream pinning guarantees all operators using given stream, will be placed on given resourceId
     * @param streamResourcePairs streamName, resourceId
     * @return physical placement plan
     */
    public List<Object> getPhysicalPlacementByStreamPinning(List<StreamResourcePair> streamResourcePairs) {
        logger.debug("Generating physical plan based on stream pinning definitions.");
        List<Object> ppPlan = new ArrayList<>();
        int opPlaced = 0;

        // get all operators
        ArrayList<Integer> compIds = neoHandler.getAllVertices("COMP");
        ArrayList<CompOperator> ops = new ArrayList<>();
        for (Integer compId : compIds) {
            ops.add(neoHandler.getOperatorById(compId));
        }

        // get all infrastructure nodes
        ArrayList<Integer> nodeIds = neoHandler.getAllVertices("NODE");
        ArrayList<InfrastructureNode> nodes = new ArrayList<>();
        for (Integer nodeId : nodeIds) {
            nodes.add(neoHandler.getInfrastructureNodeById(nodeId));
        }

        // for each node - find all computations to place on it
        for (InfrastructureNode node : nodes) {
            // find the pinned stream(s) to this comp
            List<String> streamName = getPinnedStreamName(node.getResourceId(), streamResourcePairs);
            for (CompOperator op : ops) {
                // ToDo replace is potentionally dangerous move - it is intended to include nested generated stream names
                if (streamName.contains(op.getStreamOrigin().replaceAll("_",""))) {
                    // this operator should be pinned to this node
                    ppPlan.add(op);
                    opPlaced ++;
                }
            }
            // place the actual node
            ppPlan.add(node);
        }

        // verify all operators were placed
        if (opPlaced != compIds.size()) {
            logger.error(String.format("%d ops were placed, %d intended.", opPlaced, compIds.size()));
        }

        return ppPlan;
    }

    /**
     * Returns a stream name(s) pinned to this operator.
     */
    private List<String> getPinnedStreamName(Long resourceId, List<StreamResourcePair> streamResourcePairs) {
        List<String> streamNames = new ArrayList<>();
        for (StreamResourcePair streamResourcePair : streamResourcePairs) {
            if (streamResourcePair.getResourceId() - resourceId == 0) {
                streamNames.add(streamResourcePair.getStreamName());
            }
        }
        return streamNames;
    }

    /**
     * Loop through the physical placement option and return the last Infrastructure node.
     */
    private InfrastructureNode getLastInfrastructureNode(List<Object> physicalPlacement) {
        InfrastructureNode lastNode = null;
        for (Object element : physicalPlacement) {
            if (element.getClass().equals(InfrastructureNode.class)) {
                lastNode = (InfrastructureNode) element;
            }
        }
        return lastNode;
    }

    /**
     * Builds a logical plan representation from the current state within the neo4j
     * @param udfs
     */
    public void buildLogicalPlan(UdfDefs udfs) {
        logicalPlan = new LogicalPlan(neoHandler, udfs);

        // add the first plan to the collection
        enumeratedLogicalPlans.add(logicalPlan);
    }

    /**
     * @return number of logical plans
     */
    public int getLogicalPlanCount() {
        return enumeratedLogicalPlans.size();
    }

    /**
     * @return number of operators
     */
    public int getOpCount() {
        return logicalPlan.getOpCount();
    }

    /**
     * Applies given optimisation technique on initial logical plan,
     * generating additional plan options.
     */
    public ArrayList<LogicalPlan> applyLogicalOptimisation(LogicalPlan logicalPlan, String optiTechnique) {
        switch (optiTechnique) {
            case "win":
                return windowOptimisation(logicalPlan);
            default:
                logger.error(String.format("Given logical optimisation: %s is not supported.", optiTechnique));
        }
        return null;
    }

    /**
     * Applies window optimisation technique:
     * * scans for a window in the logical plan
     * * creates a new logical plans by pushing the window closer to the data source
     * @param logicalPlan logical plan to optimise
     */
    private ArrayList<LogicalPlan> windowOptimisation(LogicalPlan logicalPlan) {
        ArrayList<LogicalPlan> optimisedPlans = new ArrayList<>();
        if (logicalPlan.hasWindows()) {
            windowOptimisationLoop(logicalPlan, optimisedPlans);
        }
        return optimisedPlans;
    }

    /**
     * Recursively examine the logical plan until the source node is reached,
     * generating new logical plans.
     * @param logicalPlan plan to be examined
     * @param optimisedPlans list of all generated plans
     */
    private void windowOptimisationLoop(LogicalPlan logicalPlan, ArrayList<LogicalPlan> optimisedPlans) {
        // generate more plans
        CompOperator winOp = logicalPlan.getOperator(0, "win");
        LogicalPlan newLogPlan = logicalPlan.pushOperatorUpstream(winOp);
        if (newLogPlan != null) {
            logger.info("New logical plan: " + newLogPlan);
            optimisedPlans.add(newLogPlan);

            // try to optimise the new plan
            windowOptimisationLoop(newLogPlan, optimisedPlans);
        }
    }

    /**
     * @return initial logical plan
     */
    public LogicalPlan getInitialLogicalPlan() {
        return logicalPlan;
    }

    /**
     * Adds supplied plans to the internal list.
     * @param logicalPlans plans to be added
     */
    public void appendLogicalPlans(ArrayList<LogicalPlan> logicalPlans) {
        enumeratedLogicalPlans.addAll(logicalPlans);
    }

    /**
     * Adds provided physical plans to the list.
     * @param physicalPlans plans to be added
     */
    public void appendPhysicalPlans(ArrayList<PhysicalPlan> physicalPlans) {
        enumeratedPhysicalPlans.addAll(physicalPlans);
    }

    /**
     * @return number of physical plans in the collection
     */
    public int getPhysicalPlanCount() {
        return enumeratedPhysicalPlans.size();
    }

    /**
     * Enumerates all possible physical plans from the provided logical plan.
     * @param logicalPlan logical plan to be turned into physical plans
     * @return collection of physical plans
     */
    public ArrayList<PhysicalPlan> placeLogicalPlan(LogicalPlan logicalPlan, InfrastructurePlan infra) {
        PhysicalPlacementGenerator ppGen = new PhysicalPlacementGenerator();
        return ppGen.generatePhysicalPlans(logicalPlan, infra);
    }

    public ArrayList<PhysicalPlan> getPhysicalPlans() {
        return enumeratedPhysicalPlans;
    }

    public ArrayList<LogicalPlan> getLogicalPlans() {
        return enumeratedLogicalPlans;
    }

    /**
     * Scans through the all physical plans and verifies that all operators
     * can be deployed on each of the selected resources.
     * Non-deployable physical plan is:
     * - a plan that has an operator placed on a resource that doesn't support that operation
     * - that is pretty much it..
     */
    public void pruneNonDeployablePhysicalPlans() {
        ArrayList<PhysicalPlan> deployablePhysicalPlans = new ArrayList<>();
        for (PhysicalPlan enumeratedPhysicalPlan : enumeratedPhysicalPlans) {
            if (isDeployble(enumeratedPhysicalPlan)) {
                deployablePhysicalPlans.add(enumeratedPhysicalPlan);
            }
        }
        enumeratedPhysicalPlans = deployablePhysicalPlans;
    }

    /**
     * Checks whether given plan is deployable.
     */
    private boolean isDeployble(PhysicalPlan physicalPlan) {
        // todo for each operator, check that platform has the capability defined in the current state
        for (CompOperator op : physicalPlan.getCurrentOps()) {
            // get the resource
            InfrastructureNode node = physicalPlan.getOpPlacementNode(op);
            if (!node.canRun(op.getType(), op.getOperator())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Finds the cheapest plan from the collection of enumerated plans.
     */
    public PhysicalPlan getExecutionPlan() {
        double minCost = 9.9e10;
        PhysicalPlan execPlan = null;
        for (PhysicalPlan physicalPlan : enumeratedPhysicalPlans) {
            if (physicalPlan.getEnergyCost() < minCost) {
                execPlan = physicalPlan;
                minCost = physicalPlan.getEnergyCost();
            }
        }
        return execPlan;
    }
}
