package eu.uk.ncl.di.pet5o.PATH2iot.infrastructure;

import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.input.network.ConnectionDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.NeoHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains all infrastructure nodes connected in neo4j.
 *
 * @author Peter Michalak
 */
public class InfrastructurePlan {
    private static Logger logger = LogManager.getLogger(InfrastructurePlan.class);

    private NeoHandler neoHandler;
    private int nodeCount;
    private InfrastructureDesc infra;
    private ArrayList<InfrastructureNode> nodes;
    private ArrayList<InfrastructureNode> firstNodes;

    public InfrastructurePlan(NeoHandler neoHandler, InfrastructureDesc infra) {
        this.neoHandler = neoHandler;
        this.infra = infra;

        // get all infrastructure nodes
        nodeCount = buildGraph();
        logger.debug(String.format("Infrastructure plan was build with %d nodes. First node(s):", nodeCount));

        // find first nodes - definition a node without incoming CONNECT stream
        firstNodes = getFirstNodes();
        for (InfrastructureNode firstNode : firstNodes) {
            logger.debug("- " + firstNode);
        }

        // update infra with nodeIds
        for (InfrastructureNode neoNode : nodes) {
            for (InfrastructureNode infraNode : infra.getNodes()) {
                if (neoNode.getResourceId().equals(infraNode.getResourceId())) {
                    infraNode.setNodeId(neoNode.getNodeId());
                }
            }
        }
    }

    private ArrayList<InfrastructureNode> getFirstNodes() {
        ArrayList<InfrastructureNode> firsts = new ArrayList<>();
        for (InfrastructureNode node : nodes) {
            boolean isFirst = true;
            for (InfrastructureNode tempNode : nodes) {
                if (tempNode.getDownstreamNodes().contains(node.getNodeId())) {
                    isFirst = false;
                }
            }
            if (isFirst) {
                firsts.add(node);
            }
        }
        return firsts;
    }

    private int buildGraph() {
        int nodeCount = 0;
        ArrayList<Integer> nodeIds = neoHandler.getAllVertices("NODE");
        nodes = new ArrayList<>();
        for (Integer nodeId : nodeIds) {
            InfrastructureNode node = neoHandler.getInfrastructureNodeById(nodeId);
            node.setDownstreamNodes(neoHandler.findDownstreamNodes(nodeId, "NODE", "NODE",
                    "CONNECTS", 1));
            nodes.add(node);
            nodeCount++;
        }
        return nodeCount;
    }

    public ArrayList<InfrastructureNode> getNodes() {
        return nodes;
    }


    public List<InfrastructureNode> getDownstreamNodes(InfrastructureNode node) {
        // examine downstream links, if not added already - add them
        List<InfrastructureNode> downstreamNodes = new ArrayList<>();
        // TODO this can be done internally - more comfy with neo CYPHER queries though.. plain lazy
        ArrayList<Integer> downstreamNodeIds = neoHandler.findAllDownstreamNodes(node.getNodeId(),
                "NODE", "NODE", "CONNECTS");
        for (Integer nodeId : downstreamNodeIds) {
            if (nodeId != node.getNodeId()) {
                downstreamNodes.add(getNodeById(nodeId));
            }
        }

        return downstreamNodes;
    }

    public InfrastructureNode getNodeById(Integer nodeId) {
        for (InfrastructureNode node : nodes) {
            if (node.getNodeId() == nodeId)
                return node;
        }
        return null;
    }

    /**
     * Retrieves the bandwidth limit from first node to second (in Bytes).
     */
    public double getBandwidthLimit(InfrastructureNode node, InfrastructureNode downstreamNode) {
        for (InfrastructureNode infraNode : infra.getNodes()) {
            if (infraNode.getResourceId().equals(node.getResourceId())) {
                for (ConnectionDesc connectionDesc : infraNode.getConnections()) {
                    if (connectionDesc.getDownstreamNode().equals(downstreamNode.getResourceId())) {
                        return connectionDesc.getBandwidth();
                    }
                }
            }
        }
        logger.warn(String.format("Bandwidth limit between %d and %d is missing! " +
                "Check your infrastructure description!", node.getResourceId(), downstreamNode.getResourceId()));
        return -1;
    }

    /**
     * Checks whether the downstream node is an immediate neighbour of parent node.
     */
    public boolean isDownstream(InfrastructureNode parentNode, InfrastructureNode downstreamNode) {
        for (Integer nodeId : parentNode.getDownstreamNodes()) {
            if (nodeId == downstreamNode.getNodeId())
                return true;
        }
        return false;
    }
}
