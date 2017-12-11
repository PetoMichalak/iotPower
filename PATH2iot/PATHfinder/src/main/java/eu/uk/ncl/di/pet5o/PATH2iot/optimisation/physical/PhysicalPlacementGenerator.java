package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical;

import eu.uk.ncl.di.pet5o.PATH2iot.infrastructure.InfrastructurePlan;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical.LogicalPlan;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
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
                PhysicalPlan newPhysPlan = new PhysicalPlan(logPlan);
                newPhysPlan.place(op, node, infra);

                // validate that the plan is complete
                if (newPhysPlan.isComplete()) {
                    ppCatalogue.add(newPhysPlan);
                } else {
                    placeRemainingOperators(newPhysPlan, infra, ppCatalogue);
                }
            }
        }

        return ppCatalogue;
    }

    /**
     * Another take on placing all operators on available infrastructure.
     */
    private void placeRemainingOperators(PhysicalPlan physPlan, InfrastructurePlan infra, ArrayList<PhysicalPlan> ppCatalogue) {
        // get remaining operators
        CompOperator unplacedOp = physPlan.getUnplacedOperators();

        // get placement nodes
        List<InfrastructureNode> placementNodes = physPlan.getPlacementNodes(unplacedOp, infra);

        for (InfrastructureNode placementNode : placementNodes) {
            // place remaining operator
            PhysicalPlan newPhysPlan = physPlan.getCopy();

            // check that the physical plan is complete
            newPhysPlan.place(unplacedOp, placementNode, infra);

            if (newPhysPlan.isComplete()) {
                ppCatalogue.add(newPhysPlan);
            } else {
                placeRemainingOperators(newPhysPlan, infra, ppCatalogue);
            }
        }
    }
}