package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical;

import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import junit.framework.TestCase;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;

public class LogicalPlanTest extends TestCase {

    private static Logger logger = LogManager.getLogger(LogicalPlanTest.class);

    /**
     * Validates correct functionality when optimising logical plan
     * - pushing windows closer to the data source.
     */
    public void testPushOperatorUpstream() throws Exception {
        // get a logical plan with window
        LogicalPlan logicalPlan = new LogicalPlan();

        // setup a SELECT comp operator with node id 0
        CompOperator tempSelectOp = new CompOperator(0);
        tempSelectOp.setName("SELECT");
        tempSelectOp.setType("RelationalOpExpression");
        tempSelectOp.setDownstreamOpIds(new ArrayList<Integer> () {{add(1);}});

        // setup a Math.pow comp operator with node id 1
        CompOperator tempMathOp = new CompOperator(1);
        tempMathOp.setName("Math.pow");
        tempMathOp.setType("RelationalOpExpression");
        tempMathOp.setDownstreamOpIds(new ArrayList<Integer> () {{add(2);}});

        // setup a window comp operator with node id 1
        CompOperator winOp = new CompOperator(2);
        winOp.setName("window");
        winOp.setType("win");
        winOp.setDownstreamOpIds(new ArrayList<Integer> ());

        // add operators to the logical plan
        logicalPlan.addOperator(tempSelectOp);
        logicalPlan.addOperator(tempMathOp);
        logicalPlan.addOperator(winOp);

        // push the window closer to the data source
        LogicalPlan newLogicalPlan = logicalPlan.pushOperatorUpstream(winOp);
        CompOperator newWinOp = newLogicalPlan.getOperator(2);
        ArrayList<Integer> downstreamOpIds = newWinOp.getDownstreamOpIds();

        // verify that the window has been moved
        assertTrue(downstreamOpIds.contains(1));
    }

}