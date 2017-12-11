package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import com.google.gson.Gson;
import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreams;
import eu.uk.ncl.di.pet5o.PATH2iot.input.udfs.UdfDefs;
import junit.framework.TestCase;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;

public class OperatorHandlerTest extends TestCase {

    private static Logger logger = LogManager.getLogger(OperatorHandlerTest.class);
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
     * Injects two nodes to the neo4j and builds logical plan out of the state.
     */
    public void testBuildLogicalPlan() throws Exception {
        // get operators to neo4j
        EsperSodaInspector eplInspector = new EsperSodaInspector(neoHandler);
        ArrayList<String> query = new ArrayList<>();
        query.add("INSERT INTO StepCount SELECT count(*) as steps FROM StepEvent.win:time_batch(120 sec)");
        String inputStreamString = "{\"inputStreams\": [{\"streamName\" : \"StepEvent\",\"streamProperties\" : [{\"name\": \"step\",\"type\": \"double\"}]}]}";
        String udfString = "{\"udf\": [{}]}";
        // need to let Esper know what streams it is suppose to expect "StepEvent"
        Gson gson = new Gson();
        InputStreams inputStreams = gson.fromJson(inputStreamString, InputStreams.class);
        UdfDefs udfs = gson.fromJson(udfString, UdfDefs.class);
        eplInspector.parseEpls(query, inputStreams, udfs);

        // build logical plan with only two nodes
        OperatorHandler opHandler = new OperatorHandler(neoHandler);
        opHandler.buildLogicalPlan(udfs);

        assertEquals(2, opHandler.getOpCount());
    }

}