package eu.uk.ncl.di.pet5o.PATH2iot;

import eu.uk.ncl.di.pet5o.PATH2iot.compile.PathCompiler;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.cost.EnergyImpactEvaluator;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.logical.LogicalPlan;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical.PhysicalPlan;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * PATHfinder is a self-contained module of PATH2iot system.
 *
 * Functionality:
   * EPL decomposition,
   * Non-functional requirements parsing
   * Energy model evaluation
   * Device specific compilation
     * Pebble Watch
     * iPhone
     * Esper node (via d2ESPer)
 *
 * @author Peter Michalak
 *
 * Requires:
   * configuration file (usually input/pathFinder.conf)
 *
 */
public class App 
{
    private static Logger logger = LogManager.getLogger(App.class);

    private static InputHandler inputHandler;
    private static NeoHandler neoHandler;
    private static EsperSodaInspector eplInspector;
    private static OperatorHandler opHandler;
    private static InfrastructureHandler infraHandler;
    private static RequirementHandler reqHandler;
    private static SocketClientHandler socketHandler;

    public static void main( String[] args )
    {
        // 0a parse input files
        inputHandler = new InputHandler(args);

        // 0b init handlers - neo, infra
        initInternalHandlers(inputHandler);

        // 1a decompose EPLs
        eplInspector.parseEpls(inputHandler.getEpls(inputHandler.getEplFile()),
                inputHandler.getInputStreams(), inputHandler.getUdfs());

        // 1b build graph of infrastructure
        infraHandler = new InfrastructureHandler(inputHandler.getInfrastructureDescription(), neoHandler);

        // 1c build graph of operators - logical plan
        opHandler.buildLogicalPlan(inputHandler.getUdfs());

        // 2a optimise logical plan
        opHandler.appendLogicalPlans(opHandler.applyLogicalOptimisation(opHandler.getInitialLogicalPlan(), "win"));
        logger.info(String.format("[pushing windows] There are %d logical plans.", opHandler.getLogicalPlanCount()));
        // todo push projects closer to the data source
        // todo inject windows

        // 2a enumerate physical plans
        for (LogicalPlan logicalPlan : opHandler.getLogicalPlans()) {
            opHandler.appendPhysicalPlans(opHandler.placeLogicalPlan(logicalPlan,
                    infraHandler.getInfrastructurePlan()));
        }
        logger.info(String.format("[generating phys plans] There are %d physical plans in the collection.", opHandler.getPhysicalPlanCount()));

        // 2b prune physical plans
        opHandler.pruneNonDeployablePhysicalPlans();
        opHandler.applyWinSafetyRules();
        logger.info(String.format("[pruning non-deployable plans] There are %d physical plans in the collection.", opHandler.getPhysicalPlanCount()));

        // 3 energy model eval
        EnergyImpactEvaluator eiEval = new EnergyImpactEvaluator(infraHandler.getInfrastructurePlan(),
                inputHandler.getEIcoeffs());
        for (PhysicalPlan physicalPlan : opHandler.getPhysicalPlans()) {
            logger.debug(String.format("%.2f EI: %s", eiEval.evaluate(physicalPlan), physicalPlan));
        }

        // list all plans that comply with the energy requirements
        double energyReq = reqHandler.getRequirement("PebbleWatch", "hour");
        int compliantPlanCount = 0;
        for (PhysicalPlan physicalPlan : opHandler.getPhysicalPlans()) {
            if (physicalPlan.getEstimatedLifetime(inputHandler.getInfrastructureDescription(), "PebbleWatch") > energyReq) {
                // this is a physical plan that complies with the energy requirements
                compliantPlanCount++;
            }
        }
        logger.info(String.format("There are %d physical plans that satisfy energy requirements (%s h).",
                compliantPlanCount, energyReq));

        // return the execution plan based on the cost
        PhysicalPlan executionPlan = opHandler.getExecutionPlan();
        logger.info(String.format("The cheapest plan is (EI: %.2f):\n%s. Estimated battery life of: %.2f hours",
                executionPlan.getEnergyCost(), executionPlan,
                executionPlan.getEstimatedLifetime(inputHandler.getInfrastructureDescription(), "PebbleWatch")));

        // 4 compile execution plan
        PathCompiler pathCompiler = new PathCompiler();
        pathCompiler.compile(executionPlan, eplInspector,
                inputHandler.getInfrastructureDescription(), inputHandler.getInputStreams());
        logger.debug(pathCompiler.getExecutionPlan());

        // 5 send the plan to PATHdeployer
        socketHandler = new SocketClientHandler(inputHandler.getPathDeployerIp(), inputHandler.getPathDeployerPort());
        socketHandler.connect();
        socketHandler.send(pathCompiler.getExecutionPlan());

        logger.info("It is done.");
    }


    /**
     * Initialisation of
     * * neo4j handler - establish connection, clean the db
     * * eplInspector - init the ESPer CEP engine
     * * operator handler - logical and physical plan optimisation module
     */
    private static void initInternalHandlers(InputHandler inputHandler) {
        neoHandler = new NeoHandler(inputHandler.getNeoAddress() + ":" + inputHandler.getNeoPort(),
                inputHandler.getNeoUser(), inputHandler.getNeoPass());
        eplInspector = new EsperSodaInspector(neoHandler);
        opHandler = new OperatorHandler(neoHandler);
        reqHandler = new RequirementHandler(inputHandler.getRequirements());
    }
}