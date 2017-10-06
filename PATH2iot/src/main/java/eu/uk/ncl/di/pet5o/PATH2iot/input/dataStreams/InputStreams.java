package eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams;

import java.util.List;

/**
 * Created by peto on 20/02/2017.
 *
 * Root definition of input_streams for gson parser.
 */
public class InputStreams {
    private List<InputStreamEntry> inputStreams;
    public InputStreams() {}

    private InputStreams(List<InputStreamEntry> inputStreams) {
        this.inputStreams = inputStreams;
    }

    public List<InputStreamEntry> getInputStreams() {
        return inputStreams;
    }

    public void setInputStreams(List<InputStreamEntry> inputStreams) {
        this.inputStreams = inputStreams;
    }

    public InputStreamEntry getInputStreamByName(String streamName) {
        for (InputStreamEntry inputStream : inputStreams) {
            if (inputStream.getStreamName().equals(streamName)) {
                return inputStream;
            }
        }
        return null;
    }


    /**
     * Returns a data type of this propName from previously recorded streams.
     * @param propName
     * @param streamName
     * @return ["integer", "double", "long", "string", "bool"]
     */
    public String getDataType(String propName, String streamName) {
        // there must be some default
        String dataType = "double";
        for (InputStreamEntry inputStream : inputStreams) {
            if (inputStream.getStreamName().equals(streamName)) {
                for (InputStreamEntryProperty streamProp : inputStream.getStreamProperties()) {
                    if (streamProp.getName().equals(propName)) {
                        return streamProp.getType();
                    }
                }
            }
        }
        return dataType;
    }

    public void addStream(InputStreamEntry newStream) {
        inputStreams.add(newStream);
    }
}
