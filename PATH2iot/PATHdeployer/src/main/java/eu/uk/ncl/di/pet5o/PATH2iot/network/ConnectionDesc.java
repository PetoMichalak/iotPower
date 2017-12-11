package eu.uk.ncl.di.pet5o.PATH2iot.network;

/**
 * Definition of connections in between the nodes.
 *
 * @author Peter Michalak
 */
public class ConnectionDesc {
    private Long downstreamNode;
    private int bandwidth;
    private double monetaryCost;
    private int bandwidthLimit;

    public ConnectionDesc(Long downstreamNode, int bandwidth, double monetaryCost) {
        this.downstreamNode = downstreamNode;
        this.bandwidth = bandwidth;
        this.monetaryCost = monetaryCost;
    }

    public Long getDownstreamNode() {
        return downstreamNode;
    }

    public void setDownstreamNode(long downstreamNode) {
        this.downstreamNode = downstreamNode;
    }

    public int getBandwidth() {
        return bandwidth;
    }

    public void setBandwidth(int bandwidth) {
        this.bandwidth = bandwidth;
    }

    public double getMonetaryCost() {
        return monetaryCost;
    }

    public void setMonetaryCost(double monetaryCost) {
        this.monetaryCost = monetaryCost;
    }

    public void setDownstreamNode(Long downstreamNode) {
        this.downstreamNode = downstreamNode;
    }

    public int getBandwidthLimit() {
        return bandwidthLimit;
    }

    public void setBandwidthLimit(int bandwidthLimit) {
        this.bandwidthLimit = bandwidthLimit;
    }
}
