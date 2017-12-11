package eu.uk.ncl.di.pet5o.PATH2iot.compile;

import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreams;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.EsperSodaInspector;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.OperatorHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;

/**
 * Created by peto on 12/03/2017.
 */
public class PebbleCompiler implements EplCompiler {

    private static Logger logger = LogManager.getLogger(PebbleCompiler.class);

    public PebbleCompiler() { }

    /**
     * Device specific configuration for a Pebble Watch
     * e.g. {"data":<int1>,"comp": <int2>, "freq":10,"queue":"AccelEvents"}
     *
     * <int1> an array of Boolean checked values to generate data
     * - an upper limit 63
     * - bit order (from LSB):
     *   - battery level (0-100 in steps of 10)
     *   - timestamp
     *   - vibe (vibration module on/off)
     *   - z (accelerometer)
     *   - y - // -
     *   - x - // -
     *
     * <int2> an array of Boolean checked values to enable/disable local processing
     * - an upper limit 7
     * - bit order (from LSB):
     *   - SELECT (vibe=0)
     *   - calcED (Euclidian distance)
     *   - sqrtOn (Newtonian approximation)
     *
     * @param ops operators to be compiled
     * @param eplInspector
     * @param infra
     * @param inputStreams
     * @return configuration for the pebble watch
     */
    @Override
    public String compile(ArrayList<CompOperator> ops, EsperSodaInspector eplInspector, InfrastructureDesc infra, InputStreams inputStreams) {
        int data = 0;
        int comp = 0;
        int freq = 0;
        int win = 0;
        String queue = "";

        // parsing all comp and data operations
        for (CompOperator op : ops) {
            switch (op.getName()) {
                case "getAccelData":
                    // set data mode and frequency (stored as in DotExpression parameter from decomposed query)
                    freq = eplInspector.getDotExpressionParam(op.getNodeId(), 0);
                    data = eplInspector.getDotExpressionParam(op.getNodeId(), 1);
                    break;
                case "SELECT":
                    switch (op.getOperator()) {
                        case "=":
                            // set the select
                            comp = comp | 0b001;
                            break;
                    }
                    break;
                case "Arithmetic":
                    // set the calcED
                    comp = comp | 0b010;
                    break;
                case "Math.pow":
                    // set the sqrtOn
                    comp = comp | 0b100;
                    break;
                case "win(120)":
                    // todo update represenation of the operator name for window -> it should only include "win"
                    // set the window size
                    win = 120;
                    break;
                default:
                    logger.error("This operator is not supported: " + op.getName());
            }
        }

        // get the final queue to output the data to
        CompOperator singleSink = OperatorHandler.getSingleSink(ops);
        queue = singleSink.getStreamDestination();

        return String.format("{\"data\":%d,\"comp\":%d,\"freq\":%d,\"queue\":\"%s\", \"win\":%d}",
                data, comp, freq, queue, win);
    }
}
