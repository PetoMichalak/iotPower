package eu.uk.ncl.di.pet5o.PATH2iot.operator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Created by peto on 14/03/2017.
 *
 * A operator definition as represented by Esper SODA API with
 * additional information to help recompile the EPL statement.
 *
 */
public class EplOperator {

    private static Logger logger = LogManager.getLogger(EplOperator.class);
    private Object esperOp;
    private EsperOpType type;

    public enum EsperOpType {
        SELECT, FROM, WHERE, MATCH_RECOGNIZE, UNKNOWN
    }

    public EplOperator(EsperOpType type, Object esperOp) {
        this.esperOp = esperOp;
        this.type = type;
    }

    public Object getEsperOp() {
        return esperOp;
    }

    public void setEsperOp(Object esperOp) {
        this.esperOp = esperOp;
    }

    public EsperOpType getType() {
        return type;
    }

    public void setType(EsperOpType type) {
        this.type = type;
    }
}
