package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.gson.Gson;
import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreams;
import eu.uk.ncl.di.pet5o.PATH2iot.input.energy.EnergyImpactCoefficients;
import eu.uk.ncl.di.pet5o.PATH2iot.input.energy.ResourceEI;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.input.udfs.UdfDefs;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Handles all input validation and information holder.
 *
 * @author Peter Michalak
 */

public class InputHandler {

    private static Logger logger = LogManager.getLogger(InputHandler.class);

    private static String NEO_ADDRESS;
    private static int NEO_PORT;
    private static String EPL_FILE;
    private static String INPUT_STREAM_FILE;
    private static String UDF_DEF_FILE;
    private static String INFRA_DEF_FILE;
    private static String STREAM_PIN_FILE;
    private static String EXEC_OUT_FILE;
    private static String ZOO_IP;
    private static int ZOO_PORT;
    private static String ZOO_ROOT_ZNODE;
    private static String REST_IP;
    private static int REST_PORT;
    private static String REST_ENDPOINT;
    private static String RESOURCEEI_DEF_FILE;

    private EnergyImpactCoefficients energyImpactCoefficients;

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
     * Load config file
     */
    private static void loadConfig(String configPath) {
        Configuration config;
        try {
            // load values from config file
            config = new PropertiesConfiguration(configPath);

            NEO_ADDRESS = config.getString("NEO_IP");
            NEO_PORT = config.getInt("NEO_PORT");
            EPL_FILE = config.getString("MASTER_QUERY_PATH");
            INPUT_STREAM_FILE = config.getString("STREAM_DEF");
            UDF_DEF_FILE = config.getString("UDF_DEF");
            INFRA_DEF_FILE = config.getString("INFRA_DEF");
            RESOURCEEI_DEF_FILE = config.getString("RESOURCE_EI");
            EXEC_OUT_FILE = config.getString("EXEC_OUT_FILE");
            ZOO_IP = config.getString("ZOO_IP");
            ZOO_PORT = config.getInt("ZOO_PORT");
            ZOO_ROOT_ZNODE = config.getString("ZOO_APP_NODE");
            REST_IP = config.getString("REST_IP");
            REST_PORT = config.getInt("REST_PORT");
            REST_ENDPOINT = config.getString("REST_ENDPOINT");

        } catch (Exception e) {
            logger.info(String.format("Error parsing the config file: %s -> \n%s",
                    configPath, e.getMessage()));
            System.exit(1);
        }
    }

    /**
     * load master EPL file into a string array for processing
     * - handles query skipping
     */
    public static ArrayList<String> getEpls(String path) {
        String [] queries = null;
        try {
            queries = Files.toString(new File(path), Charsets.UTF_8).split("\n");
        } catch (IOException e) {
            logger.error("Problem loading the set of EPL master queries: " + e.getMessage());
            System.exit(1);
        }

        // remove commented queries
        ArrayList<String> outQueries = new ArrayList<>();
        for (String query : queries) {
            if (! query.substring(0,2).equals("//") || ! query.substring(0,1).equals("#")) {
                outQueries.add(query);
            }
        }
        return outQueries;
    }

    public String getNeoAddress() {
        return NEO_ADDRESS;
    }

    public int getNeoPort() {
        return NEO_PORT;
    }

    public String getEplFile() {
        return EPL_FILE;
    }

    public String getInputStreamFile() {
        return INPUT_STREAM_FILE;
    }

    public String getUdfDefFile() {
        return UDF_DEF_FILE;
    }

    public String getInfraDefFile() {
        return INFRA_DEF_FILE;
    }

    public String getStreamPinFile() {
        return STREAM_PIN_FILE;
    }

    public String getExecOutFile() {
        return EXEC_OUT_FILE;
    }

    public String getZooIp() {
        return ZOO_IP;
    }

    public int getZooPort() {
        return ZOO_PORT;
    }

    public String getZooRootZnode() {
        return ZOO_ROOT_ZNODE;
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

    /**
     * Parses the input file to return the input streams definition.
     */
    public InputStreams getInputStreams() {
        InputStreams inputStreams = null;
        Gson gson = new Gson();
        // load input streams (if any)
        if (INPUT_STREAM_FILE.length()>0) {
            try {
                inputStreams = gson.fromJson(new FileReader(INPUT_STREAM_FILE), InputStreams.class);
                logger.info("Loaded: " + INPUT_STREAM_FILE + " loaded with " + inputStreams.getInputStreams().size() +
                        " stream/s.");
            } catch (FileNotFoundException e) {
                logger.error("Input stream file " + INPUT_STREAM_FILE + " couldn't be loaded: " + e.getMessage());
                logger.warn("Continuing ignoring this support file - bad things might happen!\n" +
                        "If you don't have any input streams (what are we processing?) leave input_streams in conf file empty...");
            }
        }
        return inputStreams;
    }

    /**
     * Parses the udf input file and returns the udf definition file.
     */
    public UdfDefs getUdfs() {
        UdfDefs udfs = null;
        Gson gson = new Gson();
        // load udfs (if any)
        if (UDF_DEF_FILE.length()>0) {
            try {
                udfs = gson.fromJson(new FileReader(UDF_DEF_FILE), UdfDefs.class);
                logger.info("Loaded: " + UDF_DEF_FILE + " loaded with " + udfs.getUdf().size() + " udf/s.");
            } catch (FileNotFoundException e) {
                logger.error("Udf file " + UDF_DEF_FILE + " couldn't be loaded: " + e.getMessage());
                logger.warn("If you don't have any UDFs leave udf_def property in conf file empty...");
            }
        }
        return udfs;
    }

    /**
     * Loads infrastructure state from JSON file
     */
    public InfrastructureDesc getInfrastructureDescription() {
        InfrastructureDesc infra = null;
        Gson gson = new Gson();
        // load infrastructure
        if (INFRA_DEF_FILE.length()>0) {
            try {
                infra = gson.fromJson(new FileReader(INFRA_DEF_FILE), InfrastructureDesc.class);
                logger.info("Loaded: " + INFRA_DEF_FILE + " loaded with " + infra.getNodes().size() + " nodes/s.");
            } catch (FileNotFoundException e) {
                logger.error("Infrastructure file " + INFRA_DEF_FILE + " couldn't be loaded: " + e.getMessage());
                logger.warn("If you don't have any infrastructure available (might be a bit tricky to do operator placement :-)) " +
                        "leave infra_def property in conf file empty...");
            }
        }
        return infra;
    }

    /**
     * Loads energy impact coefficients from the input file.
     */
    public EnergyImpactCoefficients getEIcoeffs() {
        if (energyImpactCoefficients == null) {
            // this is the first call, let's load the file
            Gson gson = new Gson();
            if (RESOURCEEI_DEF_FILE.length()>0) {
                try {
                    energyImpactCoefficients = gson.fromJson(new FileReader(RESOURCEEI_DEF_FILE), EnergyImpactCoefficients.class);
                } catch (FileNotFoundException e) {
                    logger.error("Resource EI file: " + RESOURCEEI_DEF_FILE + " couldn't be loaded: " + e.getMessage());
                    logger.warn("An energy cost model can't be evaluated without the EI coefficients!");
                }
            }
            return energyImpactCoefficients;
        } else {
            return energyImpactCoefficients;
        }
    }
}

