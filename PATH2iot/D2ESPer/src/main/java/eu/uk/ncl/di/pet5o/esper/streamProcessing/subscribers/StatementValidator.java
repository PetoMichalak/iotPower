package eu.uk.ncl.di.pet5o.esper.streamProcessing.subscribers;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.soda.*;
import eu.uk.ncl.di.pet5o.esper.input.EsperStatement;
import eu.uk.ncl.di.pet5o.esper.streamProcessing.storage.InfluxHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.List;

/**
 * Validates EPL statements to conform to the tool's capabilities.
 */
public class StatementValidator {

    private static Logger logger = LogManager.getLogger(StatementValidator.class);

    /**
     * Loops through all passed statements and validates them.
     */
    public static void validateEpls(List<EsperStatement> statements) {
        for (EsperStatement statement : statements) {
            statement.setStatement(validate(statement.getStatement()));
        }
    }

    /**
     * Based on defined validation techniques validates the EPL statement.
     */
    public static String validate(String statement){
        // check single-row function
        statement = checkSingleRow(statement);
        return statement;
    }

    /**
     * Checks whether statement is a single-row function and conforms to the parameters.
     */
    private static String checkSingleRow(String statement) {
        // init esper provider to decompose epl
        EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider(new Configuration());

        // decompose EPL
        EPStatementObjectModel eplModel = epService.getEPAdministrator().compileEPL(statement);

        // get SELECT clause
        SelectClause selectClause = eplModel.getSelectClause();

        // loop through all attributes/functions and validate them
        for (SelectClauseElement selectClauseElement : selectClause.getSelectList()) {
            // get expression
            SelectClauseExpression expr = (SelectClauseExpression) selectClauseElement;
            Expression expression = expr.getExpression();

            // single-row functions are defined as DotExpressions
            if (expression.getClass().equals(DotExpression.class)){
                // get the name
                DotExpression prop = (DotExpression) expression;

                // loop through all elements
                for (DotExpressionItem dotExpr : prop.getChain()) {
                    // get all supported single-row functions
                    List<String> singleRows = getSingleRowFunctions();
                    for (String srName : singleRows) {
                        if (dotExpr.getName().equals(srName)) {
                            logger.debug(String.format("Single-row function found: %s; validating..", srName));
                            // for each parameter within function set to
                            for (int i = 0; i < dotExpr.getParameters().size(); i++) {
                                Expression newExpr = enforceExpressionType(dotExpr.getParameters().get(i), getSingleRowExpressionType(srName, i));

                                // remove the old
                                List<Expression> parameters = dotExpr.getParameters();
                                parameters.remove(dotExpr.getParameters().get(i));

                                // insert the new
                                parameters.add(i, newExpr);

                                // replace old parameters
                                dotExpr.setParameters(parameters);
                            }
                        }
                    }
                }

            }
            epService.destroy();
        }
        return eplModel.toEPL();

    }

    /**
     * Enforces the given expression type if not compliant.
     * expected:
     * 1 - PropertyValueExpression
     * 2 - ConstantExpression
     */
    private static Expression enforceExpressionType(Expression exp, int expected) {
        switch (expected) {
            case 1:
                if (exp instanceof PropertyValueExpression) {
                    return exp;
                } else {
                    ConstantExpression tempConsExp = (ConstantExpression) exp;
                    // turn into expected expression type
                    PropertyValueExpression tempExp = new PropertyValueExpression((String) tempConsExp.getConstant());
                    return tempExp;
                }
            case 2:
                if (exp instanceof ConstantExpression) {
                    return exp;
                } else {
                    logger.warn("Unsupported conversion");
                }
        }
        logger.warn("Expression type not matched!");
        return null;
    }

    /**
     * Returns a list of all supported single-row functions.
     * todo use reflection or similar to get the list dynamically
     */
    private static List<String> getSingleRowFunctions() {
        return Arrays.asList("persistResult");
    }

    /**
     * Returns expression type for each single row function
     * todo build dynamically
     */
    private static int getSingleRowExpressionType(String srName, int parIndex) {
        switch(srName) {
            case "persistResult":
                switch(parIndex) {
                    case 0:
                        // PropertyValueExpression
                        return 1;
                    case 1:
                        // ConstantExpression
                        return 2;
                    case 2:
                        // ConstantExpression
                        return 2;
                }

                break;
            default:
                logger.warn(String.format("Definition for the single row function: %s was not found.", srName));
        }
        return -1;
    }
}
