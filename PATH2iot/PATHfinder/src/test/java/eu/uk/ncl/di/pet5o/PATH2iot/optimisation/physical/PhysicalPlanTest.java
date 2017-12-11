package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical;

import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical.LogicalPlan;
import junit.framework.TestCase;

import java.util.ArrayList;

public class PhysicalPlanTest extends TestCase {
    
    // creates a physical plan with 5 comp operators (2 source operators)
    // places all comp operators in the plan using temp nodes
    // evaluates that the algorithm correctly finds all source operators
    public void testGetSourceOps() throws Exception {
        ArrayList<CompOperator> sourceOps = new ArrayList<>();

        // create a physical plan
        PhysicalPlan physicalPlan = new PhysicalPlan(new LogicalPlan());

        // create compoperators
        CompOperator sourceOne = new CompOperator(1);
        CompOperator sourceTwo = new CompOperator(2);
        sourceOps.add(sourceOne);
        sourceOps.add(sourceTwo);
        CompOperator interOpOne = new CompOperator(11);
        CompOperator interOpTwo = new CompOperator(12);
        CompOperator interOpThree = new CompOperator(13);

        // create a connection between operators
        sourceOne.addDownstreamOp(11);
        sourceTwo.addDownstreamOp(12);
        interOpOne.addDownstreamOp(13);
        interOpTwo.addDownstreamOp(13);

        // create temp nodes for the operators to be placed on
        InfrastructureNode nodeOne = new InfrastructureNode(901);
        InfrastructureNode nodeTwo = new InfrastructureNode(902);
        InfrastructureNode nodeThree = new InfrastructureNode(903);


        // add operators to the phys plan
        physicalPlan.place(sourceOne, nodeOne, null);
        physicalPlan.place(sourceTwo, nodeTwo, null);
        physicalPlan.place(interOpOne, nodeThree, null);
        physicalPlan.place(interOpTwo, nodeThree, null);
        physicalPlan.place(interOpThree, nodeThree, null);

        // find the source operators
        ArrayList<CompOperator> sourceOpsFromPP = physicalPlan.getSourceOps();

        // assert all source operators have been returned
        assertEquals(sourceOps.size(), sourceOpsFromPP.size());

        // assert that all returned are the once created
        for (CompOperator sourceOp : physicalPlan.getSourceOps()) {
            // assert that source operators have been identified
            assertTrue(sourceOps.contains(sourceOp));
        }
    }
}