package eu.uk.ncl.di.pet5o.PATH2iot.input.udfs;

import eu.uk.ncl.di.pet5o.PATH2iot.input.operator.OperatorMetric;

import java.util.List;

/**
 * Definition for individual entries of where is this UDF supported e.g. pebble watch, iphone
 *
 * @author Peter Michalak
 */
public class UdfSupportEntry {
    private String device;
    private String version;
    private List<OperatorMetric> metrics;

    public UdfSupportEntry() {}
    public UdfSupportEntry(String device, String version, List<OperatorMetric> metrics) {
        this.device = device;
        this.version = version;
        this.metrics = metrics;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public List<OperatorMetric> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<OperatorMetric> metrics) {
        this.metrics = metrics;
    }
}
