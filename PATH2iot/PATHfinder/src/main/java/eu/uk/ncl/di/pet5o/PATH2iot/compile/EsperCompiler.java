package eu.uk.ncl.di.pet5o.PATH2iot.compile;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.google.gson.Gson;
import eu.uk.ncl.di.pet5o.PATH2iot.compile.output.*;
import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreamEntry;
import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreamEntryProperty;
import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreams;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.EsperSodaInspector;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.OperatorHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Esper compiler
 *
 * @author Peter Michalak
 */
public class EsperCompiler implements EplCompiler {

    private static Logger logger = LogManager.getLogger(EsperCompiler.class);

    EPServiceProvider epService;

    public EsperCompiler() {
        Configuration config = new Configuration();
        epService = EPServiceProviderManager.getDefaultProvider(config);
    }

    @Override
    public String compile(ArrayList<CompOperator> ops, EsperSodaInspector eplInspector, 
                          InfrastructureDesc infra, InputStreams inputStreams) {

        ArrayList<EsperStatement> statements = new ArrayList<>();
        Set<String> streamUsed = new HashSet<>();

        // for each operator
        for (CompOperator op : ops) {
            // compile based on the type of the operator
            switch(op.getType()) {
                case "DotExpression":
                    // dot expression can have nested another operators
                    CompOperator nestedOp = OperatorHandler.getUpstreamOp(op, ops);
                    eplInspector.buildDotExpression(streamUsed, statements, op, nestedOp, infra, inputStreams);
                    break;
                case "MatchRecognizeClause":
                    eplInspector.buildMatchRecognizeExpression(streamUsed, statements, op, infra, inputStreams);
                    break;
                case "CountProjectionExpression":
                    eplInspector.buildCountProjectionExpression(streamUsed, statements, op, infra, inputStreams);
                    break;
                case "UDF":
                    eplInspector.buildUdf(streamUsed, statements, op, infra, inputStreams);
                    break;
            }
        }

        // build all input streams into the configuration
        ArrayList<EsperStream> streams = new ArrayList<>();
        populateStreams(streamUsed, streams, inputStreams, infra);
        EsperDefJson config = new EsperDefJson(streams, statements);

        // format to json
        Gson gson = new Gson();
        return gson.toJson(config);
    }

    /**
     * Populates all streams that have been used within EPLs into the streams format.
     */
    private void populateStreams(Set<String> streamUsed, ArrayList<EsperStream> streams, InputStreams inputStreams, InfrastructureDesc infra) {
        for (String stream : streamUsed) {
            if (stream.length() > 0) {
                // find corresponding input stream
                InputStreamEntry inputStream = inputStreams.getInputStreamByName(stream);
                // create a stream entry for esper def json
                ArrayList<EventProperty> eventProperties = new ArrayList<>();
                for (InputStreamEntryProperty inputStreamEntryProperty : inputStream.getStreamProperties()) {
                    eventProperties.add(new EventProperty(inputStreamEntryProperty.getAsName(), inputStreamEntryProperty.getType()));
                }

                // build a queue property - message broker endpoint
                QueueProperty queue = new QueueProperty(infra.getMessageBus().getIP(), infra.getMessageBus().getPort(),
                        stream, infra.getMessageBus().getType());
                streams.add(new EsperStream(stream, 1, eventProperties, queue));
            }
        }
    }
}
