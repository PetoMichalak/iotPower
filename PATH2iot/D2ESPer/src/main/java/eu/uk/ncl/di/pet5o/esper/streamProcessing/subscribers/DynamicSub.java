package eu.uk.ncl.di.pet5o.esper.streamProcessing.subscribers;

import eu.uk.ncl.di.pet5o.esper.input.ActiveMQhandler;
import eu.uk.ncl.di.pet5o.esper.input.QueueProperty;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Created by peto on 17/12/2016.
 */
public class DynamicSub {
    private Logger logger = LogManager.getLogger(DynamicSub.class);

    private String statement;
    private ActiveMQhandler activeMQhandler;

    public DynamicSub() { }

    public DynamicSub(String statement) {
        this.statement = statement;
    }

    public DynamicSub(String statement, QueueProperty output) {
//        this.statement = StatementValidator.validate(statement);
        this.statement = statement;
        // if output is set create a connector for outgoing messages
        if (output.getType().equals("activemq") &! output.getQueue().equals("")) {
            activeMQhandler = new ActiveMQhandler(output.getIP(), output.getPort(), output.getQueue());
            logger.debug("ActiveMQ connector created for a output: " + output.getQueue());
        }
    }

    // EPL statement definition
    public String getStatement() {
        return statement;
    }

    // on condition met - send notification over ActiveMQ
    public void update(Map<String, Object> eventMap) {
        logger.info("Object arrived! " + eventMap.toString());
        if (activeMQhandler != null) {
            String out = "{";
            for (String key : eventMap.keySet()) {
                out += "\"" + key + "\":\"" + eventMap.get(key) + "\",";
            }
            out = out.substring(0,out.length()-1) + "}";
            activeMQhandler.sendMessage(out);
        }
    }

    public void close() {
        if (activeMQhandler != null) {
            activeMQhandler.close();
        }
    }
}
