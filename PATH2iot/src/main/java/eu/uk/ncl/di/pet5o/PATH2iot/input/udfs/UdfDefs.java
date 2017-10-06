package eu.uk.ncl.di.pet5o.PATH2iot.input.udfs;

import java.util.List;

/**
 * A placeholder for gson to load udf.json configuration.
 *
 * @author Peter Michalak
 */
public class UdfDefs {
    private List<UdfEntry> udf;

    public UdfDefs() {}
    public UdfDefs(List<UdfEntry> udf) {
        this.udf = udf;
    }

    public List<UdfEntry> getUdf() {
        return udf;
    }

    public void setUdf(List<UdfEntry> udf) {
        this.udf = udf;
    }
}
