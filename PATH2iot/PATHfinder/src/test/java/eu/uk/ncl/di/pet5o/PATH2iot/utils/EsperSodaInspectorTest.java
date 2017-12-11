package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.google.gson.Gson;
import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreams;
import eu.uk.ncl.di.pet5o.PATH2iot.input.udfs.UdfDefs;
import junit.framework.TestCase;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;

public class EsperSodaInspectorTest extends TestCase {

    private static Logger logger = LogManager.getLogger(EsperSodaInspectorTest.class);
    private NeoHandler neoHandler;
    private EPServiceProvider epService;

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
        // init a ESPer engine
        Configuration config = new Configuration();
        epService = EPServiceProviderManager.getDefaultProvider(config);
    }

    /**
     * Parses a query and verifies the operators were created correctly.
     */
    public void testParseEpls() throws Exception {
        // init all
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

        // there should be three nodes in a database: PROJECT:steps; COMP:COUNT; COMP:win(120)
        assertTrue(neoHandler.getNodeIdByName("steps") != -1);
        assertTrue(neoHandler.getNodeIdByName("COUNT") != -1);
        assertTrue(neoHandler.getNodeIdByName("win(120)") != -1);
    }

}