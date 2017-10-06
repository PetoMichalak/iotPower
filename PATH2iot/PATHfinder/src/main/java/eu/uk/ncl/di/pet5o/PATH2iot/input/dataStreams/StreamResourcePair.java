package eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams;

/**
 * Created by peto on 20/03/2017.
 *
 * Stream name to resource id pair class for stream pinning capability.
 */
public class StreamResourcePair {
    private String streamName;
    private Long resourceId;

    public StreamResourcePair() { }

    public String getStreamName() {
        return streamName;
    }

    public void setStreamName(String streamName) {
        this.streamName = streamName;
    }

    public Long getResourceId() {
        return resourceId;
    }

    public void setResourceId(Long resourceId) {
        this.resourceId = resourceId;
    }
}
