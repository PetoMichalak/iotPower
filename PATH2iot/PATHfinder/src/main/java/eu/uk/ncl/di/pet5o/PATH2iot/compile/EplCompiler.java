package eu.uk.ncl.di.pet5o.PATH2iot.compile;

import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreams;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.EsperSodaInspector;

import java.util.ArrayList;

/**
 * Interface for translating individual operators to the device specific language.
 *
 * @author Peter Michalak
 */
public interface EplCompiler {
//    String compile(CompOperator op, Map<Integer, EplOperator> eplOps, List<ProjectNode> projectNodes);
    String compile(ArrayList<CompOperator> ops, EsperSodaInspector eplInspector, InfrastructureDesc infra, InputStreams inputStreams);
}
