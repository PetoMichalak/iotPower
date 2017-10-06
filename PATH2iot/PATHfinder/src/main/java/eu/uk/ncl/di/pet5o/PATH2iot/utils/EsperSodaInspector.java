package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.soda.*;
import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreamEntry;
import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreamEntryProperty;
import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreams;
import eu.uk.ncl.di.pet5o.PATH2iot.input.udfs.UdfDefs;
import eu.uk.ncl.di.pet5o.PATH2iot.input.udfs.UdfEntry;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.EplOperator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by peto on 11/02/2017.
 *
 * The core of the parsing module of this project.
 * Using Esper SODA API decompose the EPL query and build
 * a graph representation of computation in NEO4J db.
 *
 */
public class EsperSodaInspector {
    private static Logger logger = LogManager.getLogger(EsperSodaInspector.class);

    private NeoHandler neoHandler;
    private Map<Integer, EplOperator> eplOps;
    private static EPServiceProvider epService;

    public EsperSodaInspector(NeoHandler neoHandler) {
        // init ESPer engine
        Configuration config = new Configuration();
        epService = EPServiceProviderManager.getDefaultProvider(config);

        // keep the neo handler
        this.neoHandler = neoHandler;

        // placeholder for operators
        eplOps = new HashMap<>();
    }

    /**
     * parses the set of loaded master queries,
     * each query is interlinked in a graph of operation
     * as defined in the input file
     *
     * @param epls set of epl master queries
     */
    public void parseEpls(ArrayList<String> epls, InputStreams inputStreams, UdfDefs udfs) {
        int lastCompNodeId = -1;
        // process EPLs
        for (String query: epls) {
            logger.info("EPL to parse: " + query);
            if (isCommented(query))
                continue;

            EplHandler eplHandle = new EplHandler(query, inputStreams, lastCompNodeId);
            EPStatementObjectModel eplModel = epService.getEPAdministrator().compileEPL(query);

            // WHERE - optional
            Expression whereClause = eplModel.getWhereClause();
            if (whereClause != null) {
                inspectWhereClause(whereClause, eplHandle);
            } else {
                logger.info("WHERE: not present");
            }

            //par FROM - must be present
            FromClause fromClause = eplModel.getFromClause();
            inspectFromClause(fromClause, eplHandle);

            // MATCH_RECOGNIZE - optional
            MatchRecognizeClause matchRecognizeClause = eplModel.getMatchRecognizeClause();
            if (matchRecognizeClause != null) {
                inspectMatchRecognize(matchRecognizeClause, eplHandle, inputStreams);
            }

            // SELECT - must be present
            SelectClause selectClause = eplModel.getSelectClause();
            inspectSelectClause(selectClause, eplHandle, udfs);

            // keep record of any streams
//            epInspector.preserveStreams(inputStreams, eplHandle.getOrigin(), eplHandle.getDestination());

            // persists the last comp node id throuhgout statements
            lastCompNodeId = eplHandle.getLastCompNodeId();

            logger.info("  --epl-done--\n");
        }
    }

    /**
     * Allows for queries to be ommited by commenting with a '#' or '//' symbol
     *
     * @param query query to be checked
     * @return true/false
     */
    private static boolean isCommented(String query) {
        // basic check first character is #
        if (query.substring(0,1).equals("#"))
            return true;
        if (query.substring(0,2).equals("//"))
            return true;
        return false;
    }

    /* **************************************
     **************** SELECT ****************
     ****************************************/

    /**
     * Examines EPL select clause and builds Computation and Projection nodes in the db.
     * @param selectClause ESPer's SODA select clause extracted from an EPL statement
     * @param eplHandle current and original stream origins and destinations
     * @param udfs external definition of all user defined functions
     */
    public void inspectSelectClause(SelectClause selectClause, EplHandler eplHandle,
                                    UdfDefs udfs) {

        // loop through all properties and represent them in neo
        for (SelectClauseElement selectClauseElement : selectClause.getSelectList()) {
            SelectClauseExpression expr = (SelectClauseExpression) selectClauseElement;
            Expression expression = expr.getExpression();

            // check if this is source udf
            if (isSourceUdf(expression, eplHandle, udfs)) {
                handleSourceUdf((DotExpression) expression, eplHandle, udfs);
                // this item has been recorded skip further processing
                continue;
            }

            // check if this is a regular udf provided by AA (application administrator)
            if (isUdf(expression, udfs)) {
                handleUdfsFromAA((DotExpression) expression, udfs, eplHandle);
                // this item has been recorded skip further processing
                continue;
            }

            // check if this is a UDF coming from Java libraries which esper loads automatically
            if (isAutoLoadedUdf(expression)) {
                handleAutoLoadedUdf(expr, eplHandle);
                // fully processed
                continue;
            }

            // is SQL-standard function used
            if (isSqlStandard(expression)) {
                handleSqlStandardFunction(expr, eplHandle);
                // fully processed
                continue;
            }

            // process all other expression types (consume them)
            processExpression(expression, eplHandle.getLastCompNodeId(), eplHandle);

            // create the project node if this is a property value expression
            if (expression instanceof PropertyValueExpression) {
                PropertyValueExpression temp_exp = (PropertyValueExpression) expression;
                int nodeId = neoHandler.createProject(temp_exp.getPropertyName(), expr.getAsName(), "double",
                        eplHandle.getOrigin(), eplHandle.getDestination());

                // link it with the computation
                neoHandler.createRel(eplHandle.getLastCompNodeId(), "STREAMS", nodeId);
            }
        }
    }

    /**
     * SQL standard functions as described here:
     * http://www.espertech.com/esper/release-5.2.0/esper-reference/html/functionreference.html#epl-function-aggregation
     */
    private boolean isSqlStandard(Expression expression) {
       if (expression.getClass().equals(CountProjectionExpression.class)) {
            return true;
       } else { // handle all other cases
           return false;
       }
    }

    /**
     * Handles Sql standard function (or subset of them).
     */
    private void handleSqlStandardFunction(SelectClauseExpression selectClauseExpression, EplHandler eplHandle) {
        Expression expression = selectClauseExpression.getExpression();
        if (expression.getClass().equals(CountProjectionExpression.class)) {
            CountProjectionExpression exp = (CountProjectionExpression) expression;

            // create a computation node
            int compId = neoHandler.createComp("COUNT", eplHandle.getOrigin(), eplHandle.getDestination(),
                    expression.getClass().getSimpleName(),
                    "COUNT",1, 1);

            // save expression for compiling stage
            eplOps.put(compId, new EplOperator(EplOperator.EsperOpType.SELECT, selectClauseExpression));

            // link up all PROJECT from the same EPL onto this node per child
            for (Expression childExp : exp.getChildren()) {
                if (childExp.getClass().equals(CrontabParameterExpression.class)) {
                    CrontabParameterExpression childCrontab = (CrontabParameterExpression) childExp;
                    // check for wildcard
                    if (childCrontab.getType().name().equals("WILDCARD")) {
                        pullAllProjects(eplHandle.getLastCompNodeId(), compId);
                    } else {
                        logger.error("This feature has not been implemented " +
                                "- pull a specific PROJECT on COUNT operator.");
                    }
                } else {
                    logger.error("This expression is not supported: " + childExp.getClass().getSimpleName());
                }
            }

            // create an output project with result
            int projectId = neoHandler.createProject(selectClauseExpression.getAsName(), selectClauseExpression.getAsName(),
                    "int", eplHandle.getOrigin(), eplHandle.getDestination());

            // create a link for the output
            neoHandler.linkNodes(compId, "COMP", projectId, "PROJECT", "STREAMS");

            // link up with previuos and update
            neoHandler.createRel(eplHandle.getLastCompNodeId(), "DOWNSTREAM", compId);
            eplHandle.setLastCompNodeId(compId);

        } else { // handle the rest of them
            logger.error("This expression is not supported: " + expression.getClass().getSimpleName());
        }
    }

    /**
     * Checks whether this is an auto-loaded Esper function:
     * * java.lang
     * * java.math
     * * java.text
     * * java.util
     * http://www.espertech.com/esper/release-5.2.0/esper-reference/html/configuration.html#config-class--package-imports
     */
    private boolean isAutoLoadedUdf(Expression expression) {
        if (! expression.getClass().equals(DotExpression.class))
            return false;

        // this is a dotexpression, let's see if it came from one of the autoloaded modules
        DotExpression expr = (DotExpression) expression;
        switch (expr.getChain().get(0).getName()) {
            case "Math":
            case "Lang":
            case "Text":
            case "Util":
                return true;
            default:
                return false;
        }
    }

    /**
     * An auto-loaded expression, handling might differ, currently handling two part math expressions.
     */
    private void handleAutoLoadedUdf(SelectClauseExpression topExpr, EplHandler eplHandle) {
        Expression expression = topExpr.getExpression();
        DotExpression expr = (DotExpression) expression;

        // handling two part Math udfs
        if ((expr.getChain().size()==2) && (expr.getChain().get(0).getName().equals("Math"))) {
            String name = expr.getChain().get(0).getName() + "." + expr.getChain().get(1).getName();

            // create a COMP representation of this node
            int nodeId = neoHandler.createComp(name, eplHandle.getOrigin(), eplHandle.getDestination(),
                    expr.getClass().getSimpleName(), name, 1, 1);

            // create the project node
            int projectNodeId = neoHandler.createProject(topExpr.getAsName(), topExpr.getAsName(),
                    eplHandle.getInputStreams().getDataType(topExpr.getAsName(), eplHandle.getOrigin()),
                    eplHandle.getOrigin(), eplHandle.getDestination());

            // link the computation to its output
            neoHandler.createRel(nodeId, "STREAMS", projectNodeId);

            // the second part has the processed elements
            DotExpressionItem udfExpr = expr.getChain().get(1);
            for (Expression param : udfExpr.getParameters()) {
                processExpression(param, nodeId, eplHandle);
            }

            // link the computation to the last comp node and update
            neoHandler.createRel(eplHandle.getLastCompNodeId(), "DOWNSTREAM", nodeId);
            eplHandle.setLastCompNodeId(nodeId);

            eplOps.put(nodeId, new EplOperator(EplOperator.EsperOpType.SELECT, topExpr));
        } else { // handle all others if and when needed
            logger.error("This type of auto-loaded udf expression is not supported.");
        }
    }

    /**
     * Handle UDF supplied by application administrator (defined in 'udfs.json') file.
     */
    private void handleUdfsFromAA(DotExpression expression, UdfDefs udfs, EplHandler eplHandle) {
        // default values
        int generationRatio = 1;
        double selectivityRatio = 1;
        String udfName = expression.getChain().get(0).getName();
        logger.debug("Processing AA UDF: " + udfName);

        // find the udf and update appropriate fields
        for (UdfEntry udf : udfs.getUdf()) {
            if (udf.getName().equals(udfName)) {
                generationRatio = udf.getGenerationRatio();
                selectivityRatio = udf.getSelectivityRatio();
            }
        }

        // create UDF node
        int nodeId = createCompNode("UDF", udfName, eplHandle.getOrigin(),
                eplHandle.getDestination(), udfName);

        // save expression for compiling stage
        eplOps.put(nodeId, new EplOperator(EplOperator.EsperOpType.SELECT, expression));

        // add property to node
        neoHandler.addProperty(nodeId, "generationRatio", generationRatio);
        neoHandler.addProperty(nodeId, "selectivityRatio", selectivityRatio);

        // examine parameters
        for (Expression param : expression.getChain().get(0).getParameters()) {
            processExpression(param, nodeId, eplHandle);
        }

        // pull previous
        pullAllProjects(eplHandle.getLastCompNodeId(), nodeId);

        // link with the last comp node and update
        neoHandler.createRel(eplHandle.getLastCompNodeId(), "DOWNSTREAM", nodeId);
        eplHandle.setLastCompNodeId(nodeId);
    }


    /**
     * Handle the UDF epxressions, that are usually connected to generating new data (placed on the IoT devices).
     */
    private void handleSourceUdf(DotExpression expression,
                                 EplHandler eplHandler, UdfDefs udfs) {
        logger.info("A source UDF detected - let's do something about it.");

        int generationRatio = 1;
        double selectivityRatio = 1.0;
        String udfName = expression.getChain().get(0).getName();

        // find the udf and update appropriate fields
        for (UdfEntry udf : udfs.getUdf()) {
            if (udf.getName().equals(udfName)) {
                generationRatio = udf.getGenerationRatio();
                selectivityRatio = udf.getSelectivityRatio();
            }
        }

        // create UDF source node
        int nodeId = createCompNode("UDF", udfName, eplHandler.getOrigin(),
                eplHandler.getDestination(), udfName);

        // persist as latest comp node
        eplHandler.setLastCompNodeId(nodeId);

        // persist the udf for compile stage
        eplOps.put(nodeId, new EplOperator(EplOperator.EsperOpType.SELECT, expression));

        // add property to node
        neoHandler.addProperty(nodeId, "generationRatio", generationRatio);
        neoHandler.addProperty(nodeId, "selectivityRatio", selectivityRatio);

        // create one node for each local source - information in input streams
        for (InputStreamEntryProperty localSourceProp :
                eplHandler.getInputStreams().getInputStreamByName(eplHandler.getOrigin()).getStreamProperties()) {
            int childNodeId = neoHandler.createProject(localSourceProp.getName(), localSourceProp.getName(),
                    localSourceProp.getType(), eplHandler.getOrigin(), eplHandler.getDestination());

            // link up to parent
            neoHandler.createRel(nodeId, "STREAMS", childNodeId);

            // link up COMP to PROJECT nodes if exist
            neoHandler.linkNodeToSource(nodeId, "COMP", "PROJECT", eplHandler.getOrigin(),
                    "asName", localSourceProp.getName(), "STREAMS");
        }

        // create input fields for any UDF arguments
        for (Expression childArgument : expression.getChain().get(0).getParameters()) {
            processExpression(childArgument, nodeId, eplHandler);
        }
    }

    /**
     * Check through UDFs and return a bool if operation (DotExpression) is found.
     */
    private boolean isUdf(Expression expression, UdfDefs udfs) {
        for (UdfEntry udf : udfs.getUdf()) {
            // if this is an udf, it should be a dotExpression, is it?
            if (expression.getClass().equals(DotExpression.class)) {
                DotExpression expr = (DotExpression) expression;
                if (expr.getChain().get(0).getName().equals(udf.getName())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Creates a project node with appropriate fields and returns the node id from neo4j.
     */
    @Deprecated
    private int createProjectNode(String name, String streamOrigin, String streamDest, String type,
                                  String propName, String propType) {
        Map<String, Object> pairs = new HashMap<>();
        pairs.put("name",  name);
        pairs.put("type", type);
        pairs.put("streamOrigin", streamOrigin);
        pairs.put("streamDestination", streamDest);
        pairs.put("generationRatio", 1);
        pairs.put("selectivityRatio", 1.0);
        pairs.put("propertyName", propName);
        pairs.put("propertyType", propType);
        return neoHandler.createNode("PROJECT", pairs);
    }

    /**
     * Build a COMP node with given parameteres and return neo4j id.
     */
    private int createCompNode(String type, String name, String streamOrigin, String streamDest, String operator) {
        Map<String, Object> pairs = new HashMap<>();
        pairs.put("name", name);
        pairs.put("type", type);
        pairs.put("streamOrigin", streamOrigin);
        pairs.put("streamDestination", streamDest);
        pairs.put("generationRatio", 1);
        pairs.put("selectivityRatio", 1.0);
        pairs.put("operator", operator);
        return neoHandler.createNode("COMP", pairs);
    }

    /**
     * Goes through the select statement are verifies whether this part of the statement belongs to an UDF.
     */
    private boolean isSourceUdf(Expression expression, EplHandler eplHandle, UdfDefs udfs) {
        Boolean originMatches = false;
        Boolean udfFuncMatches = false;

        // check whether the origin matches any entries in input_streams
        for (InputStreamEntry stream : eplHandle.getInputStreams().getInputStreams()) {
            if (stream.getStreamName().equals(eplHandle.getOrigin())) {
                originMatches = true;
            }
        }

        // check whether the function is available in udfs
        if (expression.getClass().equals(DotExpression.class)) {
            DotExpression prop = (DotExpression) expression;
            for (DotExpressionItem dotExpr : prop.getChain()) {
                for (UdfEntry udf : udfs.getUdf()) {
                    if (dotExpr.getName().equals(udf.getName())) {
                        udfFuncMatches = true;
                    }
                }
            }
        }

       return ((originMatches) && (udfFuncMatches));
    }


    /**
     * Nested dot expression handling (especially within arithmaticExpressions.
     */
    private int processDotExpression(DotExpression prop, String origin, String dest) {
        logger.info("!Computation!");
        Map<String, Object> pairs = new HashMap<>();
        pairs.put("name", "DotExp");
        pairs.put("type", prop.getClass().getSimpleName());
        pairs.put("streamOrigin", origin);
        pairs.put("streamDestination", dest);
        int nodeId = neoHandler.createNode("PROJECT", pairs);
        for (DotExpressionItem dotItem : prop.getChain()) {
            // create a comp node representing actual computation e.g. POWER
            Map<String, Object> childPairs = new HashMap<>();
            childPairs.put("name", dotItem.getName());
            childPairs.put("type", dotItem.getClass().getSimpleName());
            childPairs.put("streamOrigin", origin);
            childPairs.put("streamDestination", dest);
            childPairs.put("generationRatio", 1);
            childPairs.put("selectivityRatio", 1.0);
            childPairs.put("operation", dotItem.getName());
            int dotNodeId = neoHandler.createNode("COMP", childPairs);
            // link up to the parent node
            neoHandler.createRel(dotNodeId, "STREAMS", nodeId);

            // keep the object for the compile phase
            eplOps.put(dotNodeId, new EplOperator(EplOperator.EsperOpType.SELECT, prop));
            for (Expression param : dotItem.getParameters()) {
                if (param.getClass().equals(PropertyValueExpression.class)) {
                    PropertyValueExpression temp_exp = (PropertyValueExpression) param;
                    // create node
                    childPairs = new HashMap<>();
                    childPairs.put("name", "Project");
                    childPairs.put("type", temp_exp.getClass().getSimpleName());
                    childPairs.put("streamOrigin", origin);
                    childPairs.put("streamDestination", dest);
                    childPairs.put("propertyName", temp_exp.getPropertyName());
                    int childNodeId = neoHandler.createNode("SINK", childPairs);
                    // link up to the parent node
                    neoHandler.createRel(childNodeId, "STREAMS", dotNodeId);
                    // try to find the source
                    // TODO matching to a COMP with * operator as name
                    neoHandler.linkNodeToSource(childNodeId, "SINK", "PROJECT", origin,
                            "propertyName", temp_exp.getPropertyName(), "STREAMS");
                } else if (param.getClass().equals(ConstantExpression.class)) {
                    ConstantExpression temp_exp = (ConstantExpression) param;
                    // create node
                    childPairs = new HashMap<>();
                    childPairs.put("name", "Project");
                    childPairs.put("type", temp_exp.getClass().getSimpleName());
                    childPairs.put("streamOrigin", origin);
                    childPairs.put("streamDestination", dest);
                    childPairs.put("constant", temp_exp.getConstant().toString());
                    childPairs.put("constantType", temp_exp.getConstantType());
                    int childNodeId = neoHandler.createNode("SINK", childPairs);
                    // link up to the parent node
                    neoHandler.createRel(childNodeId, "STREAMS", dotNodeId);
                }
            }
        }
        return nodeId;
    }

    /**
     * Traverses through the select clause - focus on ArithmeticExpression and its children
     */
    private int processArithmaticExpression(ArithmaticExpression prop, String trueOrigin, String trueDest,
                                            String origin, String dest, InputStreams inputStreams) {
        logger.info("!Computation!" + prop.getOperator());

        // handle stream names in case of recursion
        // if there are no more Arithmatic Expression as children, use true origin
        if (getArithmeticExpressionCount(prop) == 0) {
            // there are no more recursions ahead so use the true origin
            origin = trueOrigin;
        }

        Map<String, Object> pairs = new HashMap<>();
        pairs.put("name", prop.getOperator());
        pairs.put("type", prop.getClass().getSimpleName());
        pairs.put("streamOrigin", origin);
        pairs.put("streamDestination", dest);
        pairs.put("generationRatio", 1);
        pairs.put("selectivityRatio", 1.0);
        pairs.put("operator", prop.getOperator());
        int nodeId = neoHandler.createNode("COMP", pairs);
        logger.info("  node created: " + nodeId + " - " + pairs);



        for (Expression expression : prop.getChildren()) {
            if (expression.getClass().equals(ArithmaticExpression.class)) {

                // we are about to go one level deeper
                int childNodeId = processArithmaticExpression((ArithmaticExpression) expression,
                        trueOrigin, trueDest, origin + "_", origin, inputStreams);

                // build a link to parent node
                neoHandler.createRel(childNodeId, "STREAMS", nodeId);
            } else if (expression.getClass().equals(PropertyValueExpression.class)) {
                PropertyValueExpression temp_exp = (PropertyValueExpression) expression;

                // create SINK and link it up to the PROJECT node
                int childNodeId = neoHandler.createSink(temp_exp.getPropertyName(),
                        inputStreams.getDataType(temp_exp.getPropertyName(), trueOrigin),
                        origin, dest);

                // link up to the parent node
                neoHandler.createRel(childNodeId, "STREAMS", nodeId);
            } else {
                logger.info("  -> default: " + origin + "/" + expression.getTreeObjectName());
            }
        }

        return nodeId;
    }

    public int getArithmeticExpressionCount(ArithmaticExpression prop) {
        int aritCount = 0;
        for (Expression expression : prop.getChildren()) {
            if (expression.getClass().equals(ArithmaticExpression.class)) {
                aritCount ++;
            }
        }
        return aritCount;
    }

    /* **************************************
     **************** WHERE *****************
     ****************************************/

    /**
     * Process the where clause - create computation and link up to previous statement
     */
    public void inspectWhereClause(Expression whereClause, EplHandler eplHandle) {
        if (whereClause.getClass().equals(RelationalOpExpression.class)) {
            RelationalOpExpression relOp = (RelationalOpExpression) whereClause;

            // create a node
            int nodeId = neoHandler.createComp("SELECT", eplHandle.getOrigin(), "",
                    relOp.getClass().getSimpleName(), relOp.getOperator(), 1, 1);

            // link to predecessor and update last computation
            neoHandler.createRel(eplHandle.getLastCompNodeId(), "DOWNSTREAM", nodeId);
            eplHandle.setLastCompNodeId(nodeId);

            // persist for compiling
            eplOps.put(nodeId, new EplOperator(EplOperator.EsperOpType.WHERE, relOp));

            for (Expression expression : whereClause.getChildren()) {
                processExpression(expression, nodeId, eplHandle);
            }
        }
    }

    /**
     * Processor of all expressions - e.g. inner parts of SELECT, FROM, and WHERE clauses.
     */
    private void processExpression(Object expression, int parentId, EplHandler eplHandle) {
        if (expression instanceof PropertyValueExpression) {
            PropertyValueExpression temp_exp = (PropertyValueExpression) expression;

            // find the source in the graph
            int sourceId = neoHandler.findSource(temp_exp.getPropertyName(), eplHandle.getOrigin());

            // link to the source node
            neoHandler.createRel(sourceId, "STREAMS", parentId);
        } else if (expression instanceof ConstantExpression) {
            ConstantExpression temp_exp = (ConstantExpression) expression;

            int nodeId = neoHandler.createProject(temp_exp.getConstant().toString(), temp_exp.getConstant().toString(),
                    temp_exp.getConstantType(), eplHandle.getOrigin(), eplHandle.getDestination());

            // link up to the parent node
            neoHandler.createRel(nodeId, "STREAMS", parentId);
        } else if (expression instanceof TimePeriodExpression) {
            TimePeriodExpression timeExp = (TimePeriodExpression) expression;
            // TODO get whether its years/months/./seconds/milliseconds
            for (Expression exp : timeExp.getChildren()) {
                if (exp.getClass().equals(ConstantExpression.class)) {
                    ConstantExpression cExp = (ConstantExpression) exp;
                    int windowLengthInSeconds = (int) cExp.getConstant();

                    // create a representation for window
                    int nodeId = neoHandler.createComp("win(" + windowLengthInSeconds + ")",
                            eplHandle.getOrigin(), eplHandle.getDestination(), "win",
                            String.valueOf(windowLengthInSeconds),1,1);

                    // link up with previous computation
                    neoHandler.createRel(parentId, "DOWNSTREAM", nodeId);

                    // wildcard pull Projects from previous output
                    pullAllProjects(eplHandle.getLastCompNodeId(), nodeId);

                    // set as current computation
                    eplHandle.setLastCompNodeId(nodeId);
                } else {
                    logger.error("This needs to be handled: " + exp.getClass());
                }
            }
        } else if (expression.getClass().equals(ArithmaticExpression.class)) { // potentially nested arithmetic expression
            // e.g. x*x + y*y + z*z
            ArithmaticExpression prop = (ArithmaticExpression) expression;

            // create comp node
            int nodeId = neoHandler.createComp("Arithmetic", eplHandle.getOrigin(), eplHandle.getDestination(),
                    prop.getClass().getSimpleName(), String.valueOf(getLevelArithExp(prop)), 1, 1);

            // keep the handle on the expression for compiling stage
            eplOps.put(nodeId, new EplOperator(EplOperator.EsperOpType.SELECT, prop));

            // find all dependencies and link
            linkAllDependencies(prop, eplHandle, nodeId);

            // link and update last node
            neoHandler.createRel(eplHandle.getLastCompNodeId(), "DOWNSTREAM", nodeId);
            eplHandle.setLastCompNodeId(nodeId);

        } else if (expression.getClass().equals(DotExpression.class)) {
            // e.g. Math.pow()

            // create COMP node


            // set as latest comp node


            // examine inner structure (if expressions found)

        } else if (expression instanceof DotExpressionItem) {
            // cast
            DotExpressionItem exp = (DotExpressionItem) expression;

            // create Project node
            int nodeId = neoHandler.createProject(exp.getName(), exp.getName(), "double",
                    eplHandle.getOrigin(), eplHandle.getDestination());

            // link up to the parent node
            neoHandler.createRel(nodeId, "STREAMS", parentId);

        } else if (expression.getClass().equals(CountProjectionExpression.class)) {
            // aggregation functions e.g. COUNT(*)

            // create COMP node

            // set as latest comp node

            // examine inner structure (if * - handle)

        } else {
            logger.warn("This expression is not supported: " + expression.getClass().getSimpleName());
        }
    }

    /**
     * Finds all Project nodes projected from a given operator and links them to another.
     * e.g. MATCH (n:PROJECT),(m:COMP) WHERE ID(m)=11865 AND (m)-[:STREAMS]->(n) RETURN n
     */
    private void pullAllProjects(int lastCompNodeId, int currentNode) {
        // find all project nodes linked to lastCompNode
        ArrayList<Integer> projectNodeIds = neoHandler.findProjectNodes(lastCompNodeId);
        logger.debug(String.format("Found %d nodes projected from %d, to be attached to %d.",
                lastCompNodeId, projectNodeIds.size(), currentNode));

        // link them to the current node
        for (Integer projectNodeId : projectNodeIds) {
            neoHandler.createRel(projectNodeId, "STREAMS", currentNode);
        }
    }

    /**
     * Given type of the expression (nested included), creates a links to the source of the stream
     */
    private void linkAllDependencies(Object expression, EplHandler eplHandle, int nodeId) {
        if (expression instanceof ArithmaticExpression) {
            // cast
            ArithmaticExpression exp = (ArithmaticExpression) expression;

            // go deeper
            for (Expression nestedExp : exp.getChildren()) {
                linkAllDependencies(nestedExp, eplHandle, nodeId);
            }
        } else if (expression instanceof PropertyValueExpression) {
            // find in neo and link
            processExpression(expression, nodeId, eplHandle);

        } else {
            logger.warn("This expression is not supported: " + expression.getClass().getSimpleName());
        }
    }

    /**
     * Quick loop through the children what is the level of nesting for the given arithmaticexpression (not a typo.. : ))
     */
    private int getLevelArithExp(ArithmaticExpression exp) {
        int level = 1;
        for (Expression expression : exp.getChildren()) {
            level += getArithExpLevelCount(expression);
        }
        return level;
    }

    private int getArithExpLevelCount(Expression expression) {
        int subLevel = 0;
        if (expression instanceof ArithmaticExpression) {
            ArithmaticExpression temp_exp = (ArithmaticExpression) expression;
            subLevel ++;
            for (Expression nestedExp : temp_exp.getChildren()) {
                subLevel += getArithExpLevelCount(nestedExp);
            }
        }
        return subLevel;
    }

    /* **************************************
     *********** MATCH RECOGNIZE ************
     ****************************************/

    public void inspectMatchRecognize(MatchRecognizeClause matchRecognizeClause, EplHandler eplHandle,
                                      InputStreams inputStreams) {

        // create a COMP node
        int nodeId = neoHandler.createComp("MATCH_RECOGNIZE",
                eplHandle.getOrigin(), eplHandle.getDestination(),
                matchRecognizeClause.getClass().getSimpleName(),
                "MATCH_RECOGNIZE", 1, 1);

        // persist for compile phase
        eplOps.put(nodeId, new EplOperator(EplOperator.EsperOpType.MATCH_RECOGNIZE, matchRecognizeClause));

        // link up the required data streams
        for (MatchRecognizeDefine def : matchRecognizeClause.getDefines()) {
            for (Expression expression : def.getExpression().getChildren()) {
                processExpression(expression, nodeId, eplHandle);
            }
        }

        // link downstream computations and update
        neoHandler.createRel(eplHandle.getLastCompNodeId(), "DOWNSTREAM", nodeId);
        eplHandle.setLastCompNodeId(nodeId);
    }

    /* **************************************
     ***************** FROM *****************
     ****************************************/

    public void inspectFromClause(FromClause fromClause, EplHandler eplHandle) {
        for (Stream stream : fromClause.getStreams()) {
            FilterStream filterStream = (FilterStream) stream;

            // is a window?
            for (View view : filterStream.getViews()) {
                for (Expression expression : view.getParameters()) {
                    processExpression(expression, eplHandle.getLastCompNodeId(), eplHandle);
                }

                // persist for compilation phase
                eplOps.put(eplHandle.getLastCompNodeId(), new EplOperator(EplOperator.EsperOpType.FROM, stream));
            }
        }
    }

    /**
     * Preserve event structure for decomposition.
     */
    public void preserveStreams(InputStreams inputStreams, String origin, String destination) {
        // if no new events create stop
        if (destination.length() == 0)
            return;

        Map<String, Object> keyPairs = new HashMap<>();
        keyPairs.put("streamDestination", destination);
        keyPairs.put("streamOrigin", destination);

        List<String> projects = neoHandler.findNodesWithProperties("PROJECT", keyPairs, "name");

        // create new stream definition
        InputStreamEntry newStream = new InputStreamEntry();
        newStream.setStreamName(destination);
        List<InputStreamEntryProperty> newProps = new ArrayList<>();

        for (String propName : projects) {
            if (propName != null && propName.length() > 0 &! propName.equals("null"))
                newProps.add(getNewStreamProperty(propName, origin, inputStreams));
        }
        newStream.setStreamProperties(newProps);
        inputStreams.addStream(newStream);

        // check that destination was created
        for (InputStreamEntry stream : inputStreams.getInputStreams()) {
            if (stream.getStreamName().equals(destination)) {
                // it exists - check that it has at least one property
                if (stream.getStreamProperties().size() == 0) {
                    // copy the origin stream here
                    copyStreamProperties(inputStreams, stream, origin);
                }
            }
        }

    }

    private void copyStreamProperties(InputStreams inputStreams, InputStreamEntry stream, String streamOriginName) {
        // find the stream
        for (InputStreamEntry originStream : inputStreams.getInputStreams()) {
            if (originStream.getStreamName().equals(streamOriginName)) {
                // copy all of it's properties to the new stream
                List<InputStreamEntryProperty> newProps = new ArrayList<>();
                for (InputStreamEntryProperty property : originStream.getStreamProperties()) {
                    newProps.add(new InputStreamEntryProperty(property.getName(), property.getType()));
                }
                stream.setStreamProperties(newProps);
                return;
            }
        }

    }

    /**
     * Examine the stream in attempt to find the match from old stream definition.
     */
    private InputStreamEntryProperty getNewStreamProperty(String propName, String streamName, InputStreams inputStreams) {
        logger.debug("Matching: " + propName + " for " + streamName);
        InputStreamEntryProperty newProp = new InputStreamEntryProperty();
        newProp.setName(propName);
        // a default value - if no match found from a previous stream
        newProp.setType("double");
        // find the stream
        for (InputStreamEntry inputStreamEntry : inputStreams.getInputStreams()) {
            if (inputStreamEntry.getStreamName().equals(streamName)) {
                // found the stream, now find the property
                for (InputStreamEntryProperty oldProp : inputStreamEntry.getStreamProperties()) {
                    if (oldProp.getName().equals(propName)) {
                        newProp.setType(oldProp.getType());
                    }
                }
            }
        }
        return newProp;
    }

    public Map<Integer, EplOperator> getEplOps() {
        return eplOps;
    }

    public void setEplOps(Map<Integer, EplOperator> eplOps) {
        this.eplOps = eplOps;
    }

}
