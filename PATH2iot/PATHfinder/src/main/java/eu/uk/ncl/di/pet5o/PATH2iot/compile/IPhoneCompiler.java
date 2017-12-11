package eu.uk.ncl.di.pet5o.PATH2iot.compile;

import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreams;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.EsperSodaInspector;

import java.util.ArrayList;

/**
 * Created by peto on 12/03/2017.
 */
public class IPhoneCompiler implements EplCompiler {

    public IPhoneCompiler() {}

    @Override
    public String compile(ArrayList<CompOperator> ops, EsperSodaInspector eplInspector, InfrastructureDesc infra, InputStreams inputStreams) {
        int mode = 0;

        for (CompOperator op : ops) {
            switch (op.getType()) {
                /*
                 * current iPhone capabilities
                 * - transfer all datapoints to the cloud via sxfer operator
                 */
                case "sxfer":
                    mode |= 0b1;
                    break;
            }
        }

        return String.format("{\"mode\": %d}", mode);
    }

}
