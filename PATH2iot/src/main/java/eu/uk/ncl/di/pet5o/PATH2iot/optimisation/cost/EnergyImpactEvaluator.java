package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.cost;

import eu.uk.ncl.di.pet5o.PATH2iot.infrastructure.InfrastructurePlan;
import eu.uk.ncl.di.pet5o.PATH2iot.input.energy.EnergyImpactCoefficients;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical.LogicalPlan;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical.PhysicalPlan;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Map;

/**
  * Calculates cost of current energy model
  *   EI = ~ OS_{idle} + \sum_{i}^{n}{comp\_cost_{i}} +
  *          \frac{msg\_count*net\_cost + BLE_{active} * BLE_{duration}}{cycle\_length}
  *
  * @author Peter Michalak
 */
public class EnergyImpactEvaluator {

    private static Logger logger = LogManager.getLogger(EnergyImpactEvaluator.class);

    private InfrastructurePlan infraPlan;
    private EnergyImpactCoefficients eiCoeffs;

    public EnergyImpactEvaluator(InfrastructurePlan infra, EnergyImpactCoefficients eiCoeffs) {
        this.infraPlan = infra;
        this.eiCoeffs = eiCoeffs;
    }

    /**
     * Evaluates the energy cost of physical plan, based on the EI coefficients.
     * @param physicalPlan plan to be evaluated
     * @return the overall energy cost for given physical plan
     */
    public double evaluate(PhysicalPlan physicalPlan) {
        // find source operators and calculate the output data payload
        ArrayList<CompOperator> sourceOps = physicalPlan.getSourceOps();

        // update the selectivity and generation ratios
        updateCoefficients(physicalPlan);

        for (CompOperator sourceOp : sourceOps) {
            sourceOp.setDataOut(sourceOp.getGenerationRatio() * sourceOp.getSelectivityRatio());
        }

        // calculate all data outputs per window size
        calculateDataOutForAllOps(sourceOps, physicalPlan);

        // I. computation impact
        double compCost = 0;
        // add cost for running on each platform (OSidle)
        for (InfrastructureNode node : physicalPlan.getAllNodes()) {
            compCost += eiCoeffs.getCost(node.getResourceType(), "OSidle", "");
        }

        // for each node calc impact of the computation
        for (CompOperator op : physicalPlan.getCurrentOps()) {
            compCost += eiCoeffs.getCost(physicalPlan.getOpPlacementNode(op).getResourceType(),
                    op.getType(), op.getOperator());
        }

        // identify the data transfer points and calculate the networking impact based on EI formulae
        // II. networking impact
        double networkCost = 0;
        for (CompOperator op : physicalPlan.getEdgeNodes()) {
            // for each downstream operator
            for (Integer downstreamOpId : op.getDownstreamOpIds()) {
                CompOperator downstreamOp = physicalPlan.getOp(downstreamOpId);
                InfrastructureNode opNode = physicalPlan.getOpPlacementNode(op);
                InfrastructureNode downstreamOpNode = physicalPlan.getOpPlacementNode(downstreamOp);

                // # msgs = ceil(payload / bandwidth)
                int numberOfMessages = (int) Math.ceil(op.getDataOut() /
                        infraPlan.getBandwidthLimit(opNode, downstreamOpNode));

                // calc the netCost
                // msg_count * net_cost_thisOP + msg_count * net_cost_downstream_op
                double opNetworkCost = numberOfMessages * eiCoeffs.getCost(opNode.getResourceType(), "netCost", "") +
                        numberOfMessages * eiCoeffs.getCost(downstreamOpNode.getResourceType(), "netCost", "");

                // calc the bleActive - default is 1s
                // BLEduration * bleActive
                int bleActive = 1;
                int winLen = 1;
                if (op.getType().equals("win")) {
                    // this one's tricky - if the window is larger than 10 seconds - the BLEduration is 10
                    // if the window is shorter than the window is win - 1 (minus one)
                    winLen = Integer.valueOf(op.getOperator());
                    bleActive = winLen > 10 ? 10 : winLen - 1;
                }
                opNetworkCost +=  bleActive * eiCoeffs.getCost(opNode.getResourceType(), "bleActive", "") +
                        bleActive * eiCoeffs.getCost(downstreamOpNode.getResourceType(), "bleActive", "");

                // normalise to the cycle 1 s
                networkCost += opNetworkCost / winLen;
            }
        }

        // set the energy cost
        physicalPlan.setEnergyCost(compCost + networkCost);

        return physicalPlan.getEnergyCost();
    }

    /**
     * Updates the coefficients the selectivity and generation coefficients
     * for each operator.
     */
    private void updateCoefficients(PhysicalPlan physicalPlan) {
        for (CompOperator op : physicalPlan.getCurrentOps()) {
            op.setGenerationRatio(eiCoeffs.getGenerationRatio(physicalPlan.getOpPlacementNode(op).getResourceType(),
                    op.getType(), op.getOperator()));
            op.setSelectivityRatio(eiCoeffs.getSelectivityRatio(physicalPlan.getOpPlacementNode(op).getResourceType(),
                    op.getType(), op.getOperator()));
        }
    }

    /**
     * Loops through the plan and calculatest the data output for all operators
     * @param sourceOps source operators provided
     * @param physicalPlan physical plan
     */
    private void calculateDataOutForAllOps(ArrayList<CompOperator> sourceOps, PhysicalPlan physicalPlan) {
        for (CompOperator sourceOp : sourceOps) {
            // get next operator
            for (Integer downstreamOpId : sourceOp.getDownstreamOpIds()) {
                CompOperator downstreamOp = physicalPlan.getOp(downstreamOpId);

                // if the operator is a window it will buffer up the results
                double winSize = downstreamOp.getType().equals("win") ?
                        Integer.valueOf(downstreamOp.getOperator()) : 1;

                // the data out for this operator is:
                // (upstream op payload * selectivity * generation ratio) * win size
                downstreamOp.setDataOut(downstreamOp.getDataOut() + sourceOp.getDataOut() *
                                downstreamOp.getSelectivityRatio() * downstreamOp.getGenerationRatio() * winSize);

                // calc the data out for this operator's downstream operators
                ArrayList<CompOperator> tempOps = new ArrayList<>();
                tempOps.add(downstreamOp);
                calculateDataOutForAllOps(tempOps, physicalPlan);
            }
        }
    }
}
