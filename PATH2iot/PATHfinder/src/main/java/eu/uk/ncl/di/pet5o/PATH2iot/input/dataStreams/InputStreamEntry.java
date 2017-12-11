package eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams;

import java.util.List;

/**
 * Inner class for gson parser.
 *
 * @author Peter Michalak
 */
public class InputStreamEntry {
    private String streamName;
    private List<InputStreamEntryProperty> streamProperties;

    public InputStreamEntry() {}
    public InputStreamEntry(String streamName, List<InputStreamEntryProperty> streamProperties) {
        this.streamName = streamName;
        this.streamProperties = streamProperties;
    }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public List<InputStreamEntryProperty> getStreamProperties() {
        return streamProperties;
    }

    public void setStreamProperties(List<InputStreamEntryProperty> streamProperties) {
        this.streamProperties = streamProperties;
    }

    /**
     * Add stream property to the definition of the stream
     */
    public void addStreamProperty(String name, String type) {
        streamProperties.add(new InputStreamEntryProperty(name, type));
    }
}
