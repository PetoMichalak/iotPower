package eu.uk.ncl.di.pet5o.esper.util;

import com.espertech.esper.view.EventStream;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import eu.uk.ncl.di.pet5o.esper.input.ActiveMQhandler;
import eu.uk.ncl.di.pet5o.esper.input.EsperStream;
import eu.uk.ncl.di.pet5o.esper.input.EventProperty;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by peto on 19/12/2016.
 */
public class MessageProcessor {

    private static Logger logger = LogManager.getLogger(MessageProcessor.class);

    public Gson gson;
    public MessageProcessor() {
        gson = new Gson();
    }

    public List<Map<String, Object>> prepEvent(String queue, String msg, List<EsperStream> streams) {
        ArrayList<Map<String, Object>> out = new ArrayList<>();
        logger.debug("Q: " + queue + "; msg: " + msg);
        EsperStream myStream = null;
        for (EsperStream stream : streams) {
           if (stream.getEventName().equals(queue)) {
               myStream = stream;
           }
        }

        ArrayList<Map<String, Object>> eventMap = new ArrayList<>();
        // parse all items in the message to strings
        try {
            // in case of single event message
            Map<String, Object> map;
            Type eventType = new TypeToken<Map<String, Object>>() {}.getType();
            map = gson.fromJson(msg, eventType);
            eventMap.add(map);
        } catch (JsonSyntaxException e) {
            // there are multiple events within the message
            logger.debug("Couldn't parse the message as a single event, attempt to convert multiple.");
            Type eventType = new TypeToken<ArrayList<Map<String, Object>>>() {
            }.getType();
            eventMap = gson.fromJson(msg, eventType);
        }

        // prep event for esper
        for (Map<String, Object> map : eventMap) {
            Map<String, Object> event = new HashMap<String, Object>();
            for (String key : map.keySet()) {
                // there must be a better way
                // find the event property
                EventProperty myProperty = null;
                for (EventProperty prop : myStream.getEventProperties()) {
                    if (prop.getKey().equals(key)) {
                        myProperty = prop;
                        break;
                    }
                }

                // cast it right
                if (myProperty.getType().equals("double")) {
                    Object temp = map.get(key);
                    if (temp instanceof Double) {
                        event.put(key, map.get(key));
                    }
                } else if (myProperty.getType().equals("long")) {
                    Object temp = map.get(key);
                    if (temp instanceof Long) {
                        event.put(key, map.get(key));
                    } else if (temp instanceof Double) {
                        event.put(key, (long) ((Double) temp).doubleValue());
                    }
                } else if ((myProperty.getType().equals("int")) || (myProperty.getType().equals("integer"))) {
                    Object temp = map.get(key);
                    if (temp instanceof Integer) {
                        event.put(key, map.get(key));
                    } else if (temp instanceof Double) {
                        Double tempDouble = (Double) map.get(key);
                        event.put(key, (int) tempDouble.doubleValue());
                    }
                }
            }
            logger.debug("Event prepared: " + event.toString());
            out.add(event);
        }
        return out;
    }
}