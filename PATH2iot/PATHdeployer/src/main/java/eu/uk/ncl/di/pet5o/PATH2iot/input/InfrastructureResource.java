package eu.uk.ncl.di.pet5o.PATH2iot.input;

/**
 * Resource description
 *
 * @author Peter Michalak
 */
public class InfrastructureResource {
    private Double cpu;
    private Double ram;
    private Double disk;
    private Double monetaryCost;
    private Double energyImpact;
    private double exec_coef;
    private double comm_coef;
    private int securityLevel;

    public InfrastructureResource() { }

    public Double getCpu() {
        return cpu;
    }

    public void setCpu(Double cpu) {
        this.cpu = cpu;
    }

    public Double getRam() {
        return ram;
    }

    public void setRam(Double ram) {
        this.ram = ram;
    }

    public Double getDisk() {
        return disk;
    }

    public void setDisk(Double disk) {
        this.disk = disk;
    }

    public Double getMonetaryCost() {
        return monetaryCost;
    }

    public void setMonetaryCost(Double monetaryCost) {
        this.monetaryCost = monetaryCost;
    }

    public Double getEnergyImpact() {
        return energyImpact;
    }

    public void setEnergyImpact(Double energyImpact) {
        this.energyImpact = energyImpact;
    }

    public int getSecurityLevel() {
        return securityLevel;
    }

    public void setSecurityLevel(int securityLevel) {
        this.securityLevel = securityLevel;
    }

    public double getExec_coef() {
        return exec_coef;
    }

    public void setExec_coef(double exec_coef) {
        this.exec_coef = exec_coef;
    }

    public double getComm_coef() {
        return comm_coef;
    }

    public void setComm_coef(double comm_coef) {
        this.comm_coef = comm_coef;
    }
}
