package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import com.google.gson.Gson;
import eu.uk.ncl.di.pet5o.PATH2iot.input.InfrastructureDesc;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;


/**
 * Handles all input validation and information holder.
 *
 * @author Peter Michalak
 */

public class InputHandler {

    private static Logger logger = LogManager.getLogger(InputHandler.class);

    private int OPTI_SOCKET_PORT;
    private String INFRA_DEF_FILE;
    private static String ZOO_IP;
    private int ZOO_PORT;
    private String ZOO_APP_NODE;
    private String REST_IP;
    private int REST_PORT;
    private String REST_ENDPOINT;

    private InfrastructureDesc infrastructureDesc;

    public InputHandler(String[] confPath) {
        // check that an argument has been passed
        if (confPath.length < 1) {
            logger.error("A config file path is required!");
            System.exit(1);
        }

        // check that the configuration exists
        if (!new File(confPath[0]).isFile()) {
            logger.error(String.format("The configuration file '%s' doesn't exist!", confPath));
            System.exit(1);
        }

        logger.debug("Loading configuration files:");
        loadConfig(confPath[0]);
    }

    /**
     * Loads infrastructure state from JSON file
     */
    public InfrastructureDesc getInfrastructureDescription() {
        if (infrastructureDesc == null) {
            Gson gson = new Gson();
            // load infrastructure
            if (INFRA_DEF_FILE.length() > 0) {
                try {
                    infrastructureDesc = gson.fromJson(new FileReader(INFRA_DEF_FILE), InfrastructureDesc.class);
                    logger.info("Loaded: " + INFRA_DEF_FILE + " loaded with " + infrastructureDesc.getNodes().size() + " nodes/s.");
                } catch (FileNotFoundException e) {
                    logger.error("Infrastructure file " + INFRA_DEF_FILE + " couldn't be loaded: " + e.getMessage());
                }
            }
        }
        return infrastructureDesc;
    }

    /**
     * Load config file
     */
    private void loadConfig(String configPath) {
        Configuration config;
        try {
            // load values from config file
            config = new PropertiesConfiguration(configPath);
            INFRA_DEF_FILE = config.getString("INFRA_DEF");
            OPTI_SOCKET_PORT = config.getInt("OPTI_SOCKET_PORT");
            ZOO_IP = config.getString("ZOO_IP");
            ZOO_PORT = config.getInt("ZOO_PORT");
            ZOO_APP_NODE = config.getString("ZOO_APP_NODE");
            REST_IP = config.getString("REST_IP");
            REST_PORT = config.getInt("REST_PORT");
            REST_ENDPOINT = config.getString("REST_ENDPOINT");

        } catch (Exception e) {
            logger.info(String.format("Error parsing the config file: %s -> \n%s",
                    configPath, e.getMessage()));
            System.exit(1);
        }
    }

    public int getOptimiserPort() {
        return OPTI_SOCKET_PORT;
    }

    public static String getZooIp() {
        return ZOO_IP;
    }

    public int getZooPort() {
        return ZOO_PORT;
    }

    public String getZooAppNode() {
        return ZOO_APP_NODE;
    }

    public String getRestIp() {
        return REST_IP;
    }

    public int getRestPort() {
        return REST_PORT;
    }

    public String getRestEndpoint() {
        return REST_ENDPOINT;
    }
}

