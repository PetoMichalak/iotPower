package eu.uk.ncl.di.pet5o.PATH2iot.input.energy;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;

public class EnergyImpactCoefficients {

    private static Logger logger = LogManager.getLogger(EnergyImpactCoefficients.class);

    private List<ResourceEI> energyResources;
    public EnergyImpactCoefficients() { }
    public List<ResourceEI> getEnergyResources() {
        return energyResources;
    }
    public void setEnergyResources(List<ResourceEI> energyResources) {
        this.energyResources = energyResources;
    }

    /**
     * Examines the inner classes and returns the energy cost for given resource type and operator.
     * @return a cost for this operator
     */
    public double getCost(String resourceType, String opType, String operator) {
        // find the resource
        for (ResourceEI energyResource : energyResources) {
            if (energyResource.getResourceType().equals(resourceType)) {
                // find the operator type
                for (EIcoefficient eiCoeff : energyResource.getEIcoefficients()) {
                    // must match the operator or a wild card
                    if (eiCoeff.getType().equals(opType)) {
                       if (eiCoeff.getOperator().equals(operator) || (eiCoeff.getOperator().equals("*"))) {
                           // found the cost
                           return eiCoeff.getCost();
                       }
                    }
                }
            }
        }
        logger.warn(String.format("A cost for %s:%s-%s was not found. Returning -1.",
                resourceType, opType, operator));
        // cost was not found
        return -1;
    }

    public double getGenerationRatio(String resourceType, String opType, String operator) {
        // find the resource
        for (ResourceEI energyResource : energyResources) {
            if (energyResource.getResourceType().equals(resourceType)) {
                // find the operator type
                for (EIcoefficient eiCoeff : energyResource.getEIcoefficients()) {
                    // must match the operator or a wild card
                    if (eiCoeff.getType().equals(opType)) {
                        if (eiCoeff.getOperator().equals(operator) || (eiCoeff.getOperator().equals("*"))) {
                            // found the cost
                            return eiCoeff.getGenerationRatio();
                        }
                    }
                }
            }
        }
        logger.warn(String.format("A cost for %s:%s-%s was not found. Returning -1.",
                resourceType, opType, operator));
        // cost was not found
        return -1;
    }

    public double getSelectivityRatio(String resourceType, String opType, String operator) {
        // find the resource
        for (ResourceEI energyResource : energyResources) {
            if (energyResource.getResourceType().equals(resourceType)) {
                // find the operator type
                for (EIcoefficient eiCoeff : energyResource.getEIcoefficients()) {
                    // must match the operator or a wild card
                    if (eiCoeff.getType().equals(opType)) {
                        if (eiCoeff.getOperator().equals(operator) || (eiCoeff.getOperator().equals("*"))) {
                            // found the cost
                            return eiCoeff.getSelectivityRatio();
                        }
                    }
                }
            }
        }
        logger.warn(String.format("A cost for %s:%s-%s was not found. Returning -1.",
                resourceType, opType, operator));
        // cost was not found
        return -1;
    }
}
