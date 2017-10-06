package eu.uk.ncl.di.pet5o.PATH2iot.input.energy;

public class EIcoefficient {

    private String type;
    private String operator;
    private double cost;
    private double confInt;
    private double generationRatio;
    private double selectivityRatio;

    public EIcoefficient() { }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public double getCost() {
        return cost;
    }

    public void setCost(double cost) {
        this.cost = cost;
    }

    public double getConfInt() {
        return confInt;
    }

    public void setConfInt(double confInt) {
        this.confInt = confInt;
    }

    public double getGenerationRatio() {
        return generationRatio;
    }

    public double getSelectivityRatio() {
        return selectivityRatio;
    }
}
