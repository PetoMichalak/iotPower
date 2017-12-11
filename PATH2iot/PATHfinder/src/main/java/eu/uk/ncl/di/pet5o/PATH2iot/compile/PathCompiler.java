package eu.uk.ncl.di.pet5o.PATH2iot.compile;

import com.google.gson.Gson;
import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreams;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical.PhysicalPlan;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.EsperSodaInspector;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Device-specific compilation.
 * Currently supported for Pebble Steel smart watch, iPhone (background Pebble app), d2esper (cloud)
 *
 * @author Peter Michalak
 */

public class PathCompiler {

    private static Logger logger = LogManager.getLogger(PathCompiler.class);

    private HashMap<Long, String> nodeConfigs;

    public PathCompiler() {
        nodeConfigs = new HashMap<>();
    }


    /**
     * Takes in a physical plan and prepares the necessary configuration for the deployment in IoT and clouds.
     * @param physicalPlan physical plan to be compiled to device-specific configuration.
     * @param eplInspector event processing language inspector
     * @param infra description of all infrastructure nodes
     * @param inputStreams
     */
    public void compile(PhysicalPlan physicalPlan, EsperSodaInspector eplInspector,
                        InfrastructureDesc infra, InputStreams inputStreams) {
        // for each node compile
        for (InfrastructureNode node : physicalPlan.getAllNodes()) {
            String config = deviceSpecificCompilation(physicalPlan, node, eplInspector, infra, inputStreams);
            nodeConfigs.put(node.getResourceId(), config);
            logger.debug(String.format("A configuration for node id: %d(%s) has been compiled: %s",
                    node.getNodeId(), node.getResourceType(), config));
        }
    }

    /**
     * Produces a device specific configuration.
     */
    private String deviceSpecificCompilation(PhysicalPlan physicalPlan, InfrastructureNode node,
                                             EsperSodaInspector eplInspector, InfrastructureDesc infra,
                                             InputStreams inputStreams) {
        String config = "";
        ArrayList<CompOperator> ops = physicalPlan.getOpsOnNode(node);
        logger.debug(String.format("Translating operators: %s; node; %s", ops, node.getResourceType()));
        switch (node.getResourceType()) {
            case "PebbleWatch":
                // compile to device specific config
                PebbleCompiler pebbleCompiler = new PebbleCompiler();
                config = pebbleCompiler.compile(ops, eplInspector, infra, inputStreams);
                break;
            case "iPhone":
                // compile to iphone capabilities config
                IPhoneCompiler iphoneCompiler = new IPhoneCompiler();
                config = iphoneCompiler.compile(ops, eplInspector, infra, inputStreams);
                break;
            case "ESPer":
                // initialise esper compiler
                EsperCompiler esperCompiler = new EsperCompiler();
                config = esperCompiler.compile(ops, eplInspector, infra, inputStreams);

        }
        return config;
    }

    /**
     * Prepares an execution plan for each device to be send to the deployment node.
     */
    public String getExecutionPlan() {
        // prep the dictionary to be transferred
        Gson gson = new Gson();
        return gson.toJson(nodeConfigs);
    }
}