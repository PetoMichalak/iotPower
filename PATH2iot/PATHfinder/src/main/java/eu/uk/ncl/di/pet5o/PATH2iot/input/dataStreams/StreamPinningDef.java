package eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams;

import java.util.List;

/**
 * Created by peto on 20/03/2017.
 *
 * Gson object holder for stream pinning input file.
 */
public class StreamPinningDef {
    private List<StreamResourcePair> streamPinning;

    public StreamPinningDef() {}

    public List<StreamResourcePair> getStreamPinning() {
        return streamPinning;
    }

    public void setStreamPinning(List<StreamResourcePair> streamPinning) {
        this.streamPinning = streamPinning;
    }
}
