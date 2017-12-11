package eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure;

/**
 * Placeholder for the node capability of the infrastructure node:
 * * name - name of operation
 * * operator - specific operations or wild card
 * * supportsWin  - whether the operator can operate on the window of events
 */
public class NodeCapability {
    public String name;
    public String operator;
    public Boolean supportsWin;

    public NodeCapability(String name, String operator, Boolean supportsWin) {
        this.name = name;
        this.operator = operator;
        this.supportsWin = supportsWin;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public Boolean getSupportsWin() {
        return supportsWin;
    }

    public void setSupportsWin(Boolean supportsWin) {
        this.supportsWin = supportsWin;
    }
}
