package eu.uk.ncl.di.pet5o.PATH2iot.input.requirements;

/**
 * A placeholder for a requirement.
 */
public class Requirement {
    private String device;
    private String reqType;
    private double min;
    private double max;
    private String units;

    public Requirement() {}

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getReqType() {
        return reqType;
    }

    public void setReqType(String reqType) {
        this.reqType = reqType;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public String getUnits() {
        return units;
    }

    public void setUnits(String units) {
        this.units = units;
    }
}
