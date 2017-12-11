package eu.uk.ncl.di.pet5o.PATH2iot.deployment;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import eu.uk.ncl.di.pet5o.PATH2iot.input.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.input.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.InputHandler;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.http.HttpResponse;


import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Deployment agent for supported clouds and IoT platforms.
 */

public class DeploymentHandler {

    private static Logger logger = LogManager.getLogger(DeploymentHandler.class);

    private InfrastructureDesc infra;
    private ZooHandler zooHandler;
    private InputHandler inputHandler;

    public DeploymentHandler(InfrastructureDesc infrastructureDescription, ZooHandler zooHandler,
                             InputHandler inputHandler) {
        this.infra = infrastructureDescription;
        this.zooHandler = zooHandler;
        this.inputHandler = inputHandler;
    }

    /**
     * Deploys for clouds and IoT
     */
    public void deploy(String input) {
        // parse the input
        Gson gson = new Gson();
        HashMap<Long, String> nodeConfigs;
        Type type = new TypeToken<Map<Long, String>>(){}.getType();
        nodeConfigs = gson.fromJson(input, type);

        // for each component enact deployment
        for (Long resourceId : nodeConfigs.keySet()) {
            logger.info("Deploying: " + resourceId);
            // deploy on infrastructure
            String platform = getPlatform(resourceId, infra);
            switch(platform) {
                case "PebbleWatch":
                    logger.debug(String.format("Deploying %s (resourceId: %d).", platform, resourceId));
                    iotDeploy(platform, resourceId, nodeConfigs.get(resourceId));
                    break;
                case "iPhone":
                    logger.debug(String.format("Deploying %s (resourceId: %d).", platform, resourceId));
                    iotDeploy(platform, resourceId, nodeConfigs.get(resourceId));
                    break;
                case "ESPer":
                    logger.debug(String.format("Deploying %s (resourceId: %d).", platform, resourceId));
                    cloudDeploy(platform, resourceId, nodeConfigs.get(resourceId));
                    break;
                default:
                    logger.warn(String.format("'%s' (resourceID: %d) is not supported for deployment.",
                            platform, resourceId));
            }
        }
    }

    /**
     * Cloud deployment
     * - sends configuration file through ZooKeeper to d2esper instance
     */
    private void cloudDeploy(String platform, Long resourceId, String config) {
        // check that the resource id has connected
        List<String> znodes = zooHandler.getZnodes(zooHandler.getRootZnode());

        // is the target node preset
        if (znodes.contains(Long.toString(resourceId))) {
            zooHandler.setCurrentZnode(zooHandler.getRootZnode() + "/" + resourceId);
            zooHandler.setCurrentData(config);
        }
    }

    /**
     * IoT deployment
     * - sending the HTTP request to Flask for config to be stored in MariaDB
     */
    private void iotDeploy(String platform, Long resourceId, String config) {
        try {
            HttpClient httpclient = HttpClients.createDefault();
            String url = String.format("http://%s:%d/%s/%d",
                    inputHandler.getRestIp(), inputHandler.getRestPort(), inputHandler.getRestEndpoint(), resourceId);
            logger.debug("Setting pebble config: " + url);
            HttpPost httpPost = new HttpPost(url);

            // Request parameters and other properties.
            httpPost.setEntity(new StringEntity(config));

            // Execute and get the response.
            HttpResponse response = httpclient.execute(httpPost);
            logger.debug("Response: " + response.getStatusLine());

        } catch (IOException e) {
            logger.error("Failed to set the config: " + e.getMessage());
        }
    }

    /**
     * Return the platform to be deployed on.
     */
    private String getPlatform(Long resourceId, InfrastructureDesc infra) {
        for (InfrastructureNode node : infra.getNodes()) {
            if (node.getResourceId().equals(resourceId)) {
                return node.getResourceType();
            }
        }
        return "unknown";
    }
}
