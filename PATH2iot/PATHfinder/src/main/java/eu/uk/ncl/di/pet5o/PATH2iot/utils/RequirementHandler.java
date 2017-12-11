package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import eu.uk.ncl.di.pet5o.PATH2iot.input.requirements.Requirement;
import eu.uk.ncl.di.pet5o.PATH2iot.input.requirements.Requirements;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * A placeholder for all requirements. Currently support for Energy only.
 *
 * @author Peter Michalak
 */
public class RequirementHandler {

    private static Logger logger = LogManager.getLogger(RequirementHandler.class);
    private Requirements requirements;

    public RequirementHandler(Requirements requirements) {
        this.requirements = requirements;
    }

    /**
     * Returns the energy requirement for the given device.
     * @param device specify the device for which to return the requirement
     * @param units hours / minutes / seconds
     */
    public double getRequirement(String device, String units) {
        for (Requirement requirement : requirements.getRequirements()) {
            if (requirement.getDevice().equals(device)) {
                if (requirement.getUnits().equals(units))
                    return requirement.getMax();
            }
        }
        logger.error(String.format("A requirement for a device %s was not found!", device));
        return 0;
    }
}
