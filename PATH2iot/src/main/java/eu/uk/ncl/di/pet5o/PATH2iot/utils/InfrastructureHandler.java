package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import eu.uk.ncl.di.pet5o.PATH2iot.infrastructure.InfrastructurePlan;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureResource;
import eu.uk.ncl.di.pet5o.PATH2iot.input.network.ConnectionDesc;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handler for all infrastructure manipulations.
 *
 * @author: Peter Michalak
 */
public class InfrastructureHandler {
    private InfrastructureDesc infra;
    private InfrastructurePlan infrastructurePlan;
    private NeoHandler neoHandler;

    private static Logger logger = LogManager.getLogger(InfrastructureHandler.class);

    public InfrastructureHandler(InfrastructureDesc infra, NeoHandler neoHandler) {
        this.infra = infra;
        this.neoHandler = neoHandler;

        // build the infrastructure in the neo4j
        buildNodes();

        // interlock the infrastructure nodes
        connectNodes();

        // create internal representation of the nodes
        this.infrastructurePlan = new InfrastructurePlan(neoHandler, infra);
    }

    /**
     * Connects the nodes within the neo4j db using CYPHER queries
     */
    public void connectNodes() {
        for (InfrastructureNode node : infra.getNodes()) {
            for (ConnectionDesc conn: node.getConnections()) {
                Map<String, Object> props = new HashMap<>();
                props.put("bandwidth", conn.getBandwidth());
                props.put("monetaryCost", conn.getMonetaryCost());
                neoHandler.linkNodes(neoHandler.getNodeIdByResourceId(node.getResourceId().intValue(), "NODE"),
                        "NODE",
                        neoHandler.getNodeIdByResourceId(conn.getDownstreamNode().intValue(), "NODE"),
                        "NODE", props, "CONNECTS");
            }
        }
    }

    /**
     * Create all nodes as represented in the input files.
     * @return number of nodes created
     */
    public int buildNodes() {
        int nodeCount = 0;
        for (InfrastructureNode node : infra.getNodes()) {
            // only build a node if it's active
            if (node.getState().equals("active")) {
                Map<String, Object> childPairs = new HashMap<>();
                childPairs.put("resourceId", node.getResourceId().toString());
                childPairs.put("resourceType", node.getResourceType());
                // hook up resources
                InfrastructureResource infraResource = node.getResources().get(0);
                childPairs.put("cpu", infraResource.getCpu().toString());
                childPairs.put("ram", infraResource.getRam().toString());
                childPairs.put("disk", infraResource.getDisk().toString());
                childPairs.put("dataOut", -1);
                childPairs.put("monetaryCost", infraResource.getMonetaryCost().toString());
                childPairs.put("energyImpact", infraResource.getEnergyImpact().toString());
                childPairs.put("securityLevel", Integer.toString(infraResource.getSecurityLevel()));
                childPairs.put("capabilities", formatNodeCapabilities(node.getCapabilities()));

                // build a node
                neoHandler.createNode("NODE", childPairs);
                nodeCount++;
            }
        }
        return nodeCount;
    }

    /**
     * Converts a JSON list into a CSV format
     */
    private String formatNodeCapabilities(List<String> capabilities) {
        String out = "";
        for (String capability : capabilities) {
            out += capability + ",";
        }
        // get rid of trailing comma
        out = out.substring(0, out.length()-1);
        return out;
    }

    /**
     * @return infrastructure plan
     */
    public InfrastructurePlan getInfrastructurePlan() {
        return infrastructurePlan;
    }
}
