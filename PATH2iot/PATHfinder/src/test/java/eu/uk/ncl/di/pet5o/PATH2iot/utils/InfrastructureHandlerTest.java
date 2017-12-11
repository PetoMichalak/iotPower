package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import com.google.gson.Gson;
import eu.uk.ncl.di.pet5o.PATH2iot.infrastructure.InfrastructurePlan;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import junit.framework.TestCase;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.FileReader;

public class InfrastructureHandlerTest extends TestCase {

    private static Logger logger = LogManager.getLogger(InfrastructureHandlerTest.class);
    private NeoHandler neoHandler;

    public void setUp() throws Exception {
        super.setUp();
        String neoConnString = System.getProperty("neoconnectionstring");
        String neoUser = System.getProperty("neousername");
        String neoPass = System.getProperty("neopassword");
        if (neoConnString != null) {
            neoHandler = new NeoHandler(neoConnString, neoUser, neoPass);
        } else {
            logger.error("'neoconnectionstring' not supplied!");
            fail();
        }
    }

    /**
     * Loads infrastructure file and checks that the correct nodes were created.
     */
    public void testBuildNodes() throws Exception {
        Gson gson = new Gson();
        // define two nodes - one active (Pebble Watch) and one disabled (ESPer)
        String infraDescString = "{\"nodes\": [{\"state\": \"active\",\"resourceId\": 115001,\"resourceType\": \"PebbleWatch\",\"swVersion\": \"1.0.0\",\"resources\": [{\"cpu\": 40.0,\"ram\": 32.0,\"disk\": 16.0,\"monetaryCost\": 0.0,\"energyImpact\": 100,\"securityLevel\": 1}],\"connections\": [{\"downstreamNode\": 115002,\"bandwidth\":2,\"monetaryCost\":0.0  }],\"capabilities\": [\"UDF:getAccelData\", \"RelationalOpExpression:=\", \"ArithmaticExpression:*\", \"DotExpression:Math.pow\", \"win:*\"]},{\"state\": \"disabled\",\"resourceId\": 65001,\"resourceType\": \"ESPer\",\"swVersion\": \"1.0.0\",\"resources\": [{\"cpu\": 800.0,\"ram\": 4000.0,\"disk\": 16000.0,\"monetaryCost\": 0.001,\"energyImpact\": 0.001,\"securityLevel\": 1}],\"connections\": [{\"downstreamNode\": 65002,\"bandwidth\":100000,\"monetaryCost\":0.0 },{\"downstreamNode\": 65003,\"bandwidth\":100000,\"monetaryCost\":0.0 },{\"downstreamNode\": 65004,\"bandwidth\":100000,\"monetaryCost\":0.0 },{\"downstreamNode\": 65005,\"bandwidth\":100000,\"monetaryCost\":0.0 }],\"capabilities\": [\"UDF:persistResult\", \"RelationalOpExpression:*\", \"ArithmaticExpression:*\", \"DotExpression:*\", \"MatchRecognizeClause:*\", \"win:*\", \"CountProjectionExpression:*\"]}],\"messageBus\": {}}";
        InfrastructureDesc infra = gson.fromJson(infraDescString, InfrastructureDesc.class);

        // build the nodes
        InfrastructureHandler infraHandler = new InfrastructureHandler(infra, neoHandler);

        // there should be only PebbleWatch present in the infrastructure
        assertTrue(neoHandler.getNodeIdByResourceId(115001, "NODE") != -1);
        assertEquals(neoHandler.getNodeIdByResourceId(65002, "NODE"), -1);
    }

}