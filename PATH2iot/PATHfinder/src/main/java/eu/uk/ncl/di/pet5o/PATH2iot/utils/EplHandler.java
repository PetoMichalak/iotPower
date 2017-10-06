package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.soda.*;
import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreams;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Created by peto on 11/04/2017.
 *
 * A helper class to hold current state of EPL parse progress.
 * Introduced to keep track during a multi-comp decomposition within single query.
 */
public class EplHandler {

    private static Logger logger = LogManager.getLogger(EplHandler.class);
    private static EPServiceProvider epService;

    private String origin;
    private String destination;
    private int lastCompNodeId;
    private String epl;
    private InputStreams inputStreams;

    public EplHandler(String epl, InputStreams inputStreams, int lastCompNodeId) {
        this.epl = epl;
        this.inputStreams = inputStreams;
        this.lastCompNodeId = lastCompNodeId;

        // init Esper SODA
        Configuration config = new Configuration();
        epService = EPServiceProviderManager.getDefaultProvider(config);
        EPStatementObjectModel eplModel = epService.getEPAdministrator().compileEPL(epl);

        // get stream origin from FROM clause
        FromClause fromClause = eplModel.getFromClause();
        for (Stream stream : fromClause.getStreams()) {
            if (stream instanceof FilterStream) {
                FilterStream filterStream = (FilterStream) stream;
                origin = filterStream.getFilter().getEventTypeName();
            } else {
                logger.error("This type of stream is not supported: " + stream.getClass().getSimpleName());
            }
        }

        // get stream dest from INSERT - if exists
        InsertIntoClause insertInto = eplModel.getInsertInto();
        if (insertInto != null) {
            logger.info("  INSERT: " + insertInto.getStreamName());
            destination = insertInto.getStreamName();
        } else {
            logger.info("  INSERT: not present");
            destination = "";
        }
        logger.debug(this);
    }

    @Override
    public String toString() {
        return String.format("EPL: %s -> %s", origin, destination);
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public int getLastCompNodeId() {
        return lastCompNodeId;
    }

    public void setLastCompNodeId(int lastCompNodeId) {
        this.lastCompNodeId = lastCompNodeId;
    }

    public String getEpl() {
        return epl;
    }

    public void setEpl(String epl) {
        this.epl = epl;
    }

    public InputStreams getInputStreams() {
        return inputStreams;
    }

    public void setInputStreams(InputStreams inputStreams) {
        this.inputStreams = inputStreams;
    }
}
