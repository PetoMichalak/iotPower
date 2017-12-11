package eu.uk.ncl.di.pet5o.PATH2iot.compile.output;

import java.util.List;

/**
 * Created by peto on 17/12/2016.
 */
public class EsperStream {
    private String eventName;
    private int isInput;
    private List<EventProperty> eventProperties;
    private QueueProperty source;

    public EsperStream(String eventName, int isInput, List<EventProperty> eventProperties, QueueProperty source) {
        this.eventName = eventName;
        this.isInput = isInput;
        this.eventProperties = eventProperties;
        this.source = source;
    }

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public List<EventProperty> getEventProperties() {
        return eventProperties;
    }

    public void setEventProperties(List<EventProperty> eventProperties) {
        this.eventProperties = eventProperties;
    }

    public QueueProperty getSource() {
        return source;
    }

    public void setSource(QueueProperty source) {
        this.source = source;
    }

    public int getIsInput() {
        return isInput;
    }

    public void setIsInput(int isInput) {
        this.isInput = isInput;
    }
}
