package eu.uk.ncl.di.pet5o.PATH2iot.compile.output;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Created by peto on 17/12/2016.
 */
public class EsperDefJson {
    private static Logger logger = LogManager.getLogger(EsperDefJson.class);


    private List<EsperStream> streams;
    private List<EsperStatement> statements;

    public EsperDefJson() {
    }

    public EsperDefJson(List<EsperStream> streams, List<EsperStatement> statements) {
        this.streams = streams;
        this.statements = statements;
    }

    public List<EsperStream> getStreams() {
        return streams;
    }

    public void setStreams(List<EsperStream> streams) {
        this.streams = streams;
    }

    public List<EsperStatement> getStatements() {
        return statements;
    }

    public void setStatements(List<EsperStatement> statements) {
        this.statements = statements;
    }

    public void print() {
        for (EsperStream stream : getStreams()) {
            logger.info("Stream: " + stream.getEventName());
            for (EventProperty prop : stream.getEventProperties()) {
                logger.info(" Property: " + prop.getKey() + ":" + prop.getType());
            }
        }
        for (EsperStatement statement : getStatements()) {
            logger.info("Statement: " + statement.getStatement() + "; output: " + statement.getOutput());
        }
    }
}
