package eu.uk.ncl.di.pet5o.esper.input;

import java.util.List;

/**
 * Created by peto on 17/12/2016.
 */
public class EsperStatement {
    private String statement;
    private QueueProperty output;

    public EsperStatement (String Statement, QueueProperty output) {
        this.statement = Statement;
        this.output = output;
    }

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public QueueProperty getOutput() {
        return output;
    }

    public void setOutput(QueueProperty output) {
        this.output = output;
    }
}
