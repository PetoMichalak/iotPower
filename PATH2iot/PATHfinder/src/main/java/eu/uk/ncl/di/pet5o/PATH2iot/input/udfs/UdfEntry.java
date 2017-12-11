package eu.uk.ncl.di.pet5o.PATH2iot.input.udfs;

import java.util.List;

/**
 * Inner class for gson parsing of udfs.def file.
 *
 * @author Peter Michalak
 */
public class UdfEntry {
    private String name;
    private String output;
    private int frequency;
    private int generationRatio;
    private double selectivityRatio;
    private Boolean isSource;
    private String notes;
    private List<UdfSupportEntry> support;

    public UdfEntry() { }
    public UdfEntry(String name, String output, int frequency, int generationRatio, double selectivityRatio,
                    String notes, List<UdfSupportEntry> support) {
        this.name = name;
        this.output = output;
        this.frequency = frequency;
        this.generationRatio = generationRatio;
        this.selectivityRatio = selectivityRatio;
        this.notes = notes;
        this.support = support;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public String getNotes() {
        return notes;
    }

    public void setNotes(String notes) {
        this.notes = notes;
    }

    public List<UdfSupportEntry> getSupport() {
        return support;
    }

    public void setSupport(List<UdfSupportEntry> support) {
        this.support = support;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public int getGenerationRatio() {
        return generationRatio;
    }

    public void setGenerationRatio(int generationRatio) {
        this.generationRatio = generationRatio;
    }

    public double getSelectivityRatio() {
        return selectivityRatio;
    }

    public void setSelectivityRatio(double selectivityRatio) {
        this.selectivityRatio = selectivityRatio;
    }

    public boolean isSource() {
        return isSource;
    }
}
