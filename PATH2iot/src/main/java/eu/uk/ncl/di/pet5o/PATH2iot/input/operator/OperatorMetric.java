package eu.uk.ncl.di.pet5o.PATH2iot.input.operator;

/**
 * Most inner class of udfs.json used to hold metric information for given operator computation.
 *
 * @author Peter Michalak
 */
public class OperatorMetric {
    private Double cpuCost;
    private Double ramCost;
    private Double diskCost;
    private Double dataOut;
    private Double monetaryCost;
    private int securityLevel;

    public OperatorMetric() {}
    public OperatorMetric(Double cpuCost, Double ramCost, Double diskCost, Double dataOut, Double monetaryCost, int securityLevel) {
        this.cpuCost = cpuCost;
        this.ramCost = ramCost;
        this.diskCost = diskCost;
        this.dataOut = dataOut;
        this.monetaryCost = monetaryCost;
        this.securityLevel = securityLevel;
    }

    public Double getCpuCost() {
        return cpuCost;
    }

    public void setCpuCost(Double cpuCost) {
        this.cpuCost = cpuCost;
    }

    public Double getRamCost() {
        return ramCost;
    }

    public void setRamCost(Double ramCost) {
        this.ramCost = ramCost;
    }

    public Double getDiskCost() {
        return diskCost;
    }

    public void setDiskCost(Double diskCost) {
        this.diskCost = diskCost;
    }

    public Double getDataOut() {
        return dataOut;
    }

    public void setDataOut(Double dataOut) {
        this.dataOut = dataOut;
    }

    public Double getMonetaryCost() {
        return monetaryCost;
    }

    public void setMonetaryCost(Double monetaryCost) {
        this.monetaryCost = monetaryCost;
    }

    public int getSecurityLevel() {
        return securityLevel;
    }

    public void setSecurityLevel(int securityLevel) {
        this.securityLevel = securityLevel;
    }
}
