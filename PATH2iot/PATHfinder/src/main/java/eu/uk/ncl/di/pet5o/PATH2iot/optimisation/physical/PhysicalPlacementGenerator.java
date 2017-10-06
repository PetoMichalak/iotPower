package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical;

import com.google.common.collect.Collections2;
import eu.uk.ncl.di.pet5o.PATH2iot.infrastructure.InfrastructurePlan;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical.LinkedOpManager;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical.LinkedOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical.LogicalPlan;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Generates a physical placements based on passed parameters.
 *
 * @author Peter Michalak
 */
public class PhysicalPlacementGenerator {

    private static Logger logger = LogManager.getLogger(PhysicalPlacementGenerator.class);

    public PhysicalPlacementGenerator() {}

    /**
     * Takes in a logical plan and description of computation and enumerates all possible deployemnt
     * options for given plan
     * @param logPlan logical plan to be placed
     * @param infra current active infrastructure
     * @return collection of physical plans
     */
    public ArrayList<PhysicalPlan> generatePhysicalPlans(LogicalPlan logPlan, InfrastructurePlan infra) {
        // create initial plan(s) - first operator(s) placed on all infrastructure nodes
        ArrayList<PhysicalPlan> ppCatalogue = new ArrayList<>();

        // for each first operator in a logical plan
        for (CompOperator op : logPlan.getFirstOperators()) {

            // place this operator on a node (assuming computation can start anywhere)
            // the un-deployable plans will be pruned in the later step
            for (InfrastructureNode node : infra.getNodes()) {
                PhysicalPlan newPhysPlan = new PhysicalPlan();
                newPhysPlan.place(op, node);

                // for each downstream operator, place it on all the downstream nodes
                for (Integer downstreamOpId : op.getDownstreamOpIds()) {
                    CompOperator downstreamOp = logPlan.getOperator(downstreamOpId);
                    placeOperator(downstreamOp, op, newPhysPlan, logPlan, infra, ppCatalogue);
                }
            }
        }

        return ppCatalogue;
    }

    /**
     * Functions places the downstream operator on the same node as the old operator was,
     * and all downstream infrastructure nodes.
     *
     * @param op downstream operator to be placed
     * @param parentOp parent operator
     * @param physPlan old physical plan
     * @param infra infrastructure plan describing current state of nodes where operators can be placed
     * @param ppCatalogue catalogue of all physical complete plans
     */
    private void placeOperator(CompOperator op, CompOperator parentOp, PhysicalPlan physPlan, LogicalPlan logPlan,
                               InfrastructurePlan infra, ArrayList<PhysicalPlan> ppCatalogue) {
        // buffer for new physical plans
        ArrayList<PhysicalPlan> newPhysPlans = new ArrayList<>();

        // find where the parent operator is placed
        InfrastructureNode parentNode = physPlan.getOpPlacementNode(parentOp);

        // place the operator at the same place
        PhysicalPlan newPhysPlan = physPlan.getCopy();
        newPhysPlan.place(op, parentNode);
        newPhysPlans.add(newPhysPlan);

        // iterate through all downstream nodes
        for (InfrastructureNode downstreamNode : infra.getDownstreamNodes(parentNode)) {
            PhysicalPlan tempPhysPlan = physPlan.getCopy();

            // check whether the downstream node is an immediate neighbour
//            if (!infra.isDownstream(parentNode, downstreamNode)) {
//                // get the downsream node
//                List<Integer> downstreamNodes = parentNode.getDownstreamNodes();
//
//                for (Integer downstreamNodeId : downstreamNodes) {
//                    InfrastructureNode downstreamNeighbour = infra.getNodeById(downstreamNodeId);
//
//                    // create sxfer - transfer operator
//                    CompOperator sxfer = tempPhysPlan.createSxferOp();
//
//                    // sxfer operator to point where op used to
//                    sxfer.setDownstreamOpIds(op.getDownstreamOpIds());
//
//                    // local op to point at the sxfer
//                    CompOperator localOp = op.getCopy();
//                    localOp.clearDownstreamOps();
//                    localOp.addDownstreamOp(sxfer.getNodeId());
//                    tempPhysPlan.place(localOp, downstreamNode);
//
//                    // add sxfer on the downstream neighbour node
//                    tempPhysPlan.place(sxfer, downstreamNeighbour);
//                }
//            } else {
                tempPhysPlan.place(op, downstreamNode);
//            }

            // add new plan to the phys plan
            newPhysPlans.add(tempPhysPlan);
        }

        // for each downstream operator run this function
        ArrayList<Integer> downstreamOpIds = op.getDownstreamOpIds();
        for (Integer downstreamOpId : downstreamOpIds) {
            CompOperator downstreamOp = logPlan.getOperator(downstreamOpId);
            for (PhysicalPlan tempPhysPlan : newPhysPlans) {
                placeOperator(downstreamOp, op, tempPhysPlan, logPlan, infra, ppCatalogue);
            }
        }

        // if there are no downstream operators - add to ppCatalogue and return
        if (downstreamOpIds.isEmpty()) {
            ppCatalogue.addAll(newPhysPlans);
        }
    }
}
