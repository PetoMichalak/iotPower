package eu.uk.ncl.di.pet5o.esper.input;

import eu.uk.ncl.di.pet5o.esper.App;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Created by peto on 17/12/2016.
 */
public class EventProperty {
    private static Logger logger = LogManager.getLogger(EventProperty.class);

    private String key;
    private String type;

    public EventProperty(String key, String type) {
        this.key = key;
        this.type = type;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Object getKeyClass() {
        if (type.equals("double")) {
            return double.class;
        } else if (type.equals("int") || (type.equals("integer"))) {
            return int.class;
        } else if (type.equals("String")) {
            return String.class;
        } else if (type.equals("long")) {
            return long.class;
        } else if (type.equals("object")) {
            return Object.class;
        }
        logger.error("Problem with the type casting! " + key + ":" + type);
        return null;
    }
}
