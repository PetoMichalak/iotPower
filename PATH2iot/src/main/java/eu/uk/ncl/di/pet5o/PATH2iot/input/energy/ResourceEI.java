package eu.uk.ncl.di.pet5o.PATH2iot.input.energy;

import java.util.List;

public class ResourceEI {
    private String resourceType;
    private String swVersion;
    private List<EIcoefficient> EIcoefficients;

    public ResourceEI() { }

    public String getResourceType() {
        return resourceType;
    }

    public void setResourceType(String resourceType) {
        this.resourceType = resourceType;
    }

    public String getSwVersion() {
        return swVersion;
    }

    public void setSwVersion(String swVersion) {
        this.swVersion = swVersion;
    }

    public List<EIcoefficient> getEIcoefficients() {
        return EIcoefficients;
    }

    public void setEIcoefficients(List<EIcoefficient> EIcoefficients) {
        this.EIcoefficients = EIcoefficients;
    }
}
