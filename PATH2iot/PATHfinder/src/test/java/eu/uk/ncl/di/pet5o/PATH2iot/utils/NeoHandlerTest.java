package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import junit.framework.TestCase;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class NeoHandlerTest extends TestCase {

    private static Logger logger = LogManager.getLogger(NeoHandlerTest.class);
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
     * Validate connection, creation, persistance and querying of a single value property in neo4j
     */
    public void testCreateNode() throws Exception {
        Long testParam = 50L;
        int nodeId = neoHandler.createNode(String.format("CREATE (n:TEST {testParam:%d}) RETURN ID(n)", testParam));
        Long queriedParam = (Long) neoHandler.getNodeProperty2(nodeId, "TEST", "testParam");
        assertEquals(testParam, queriedParam);
    }
}