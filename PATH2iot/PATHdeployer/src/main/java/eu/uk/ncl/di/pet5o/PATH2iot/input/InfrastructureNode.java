package eu.uk.ncl.di.pet5o.PATH2iot.input;

import eu.uk.ncl.di.pet5o.PATH2iot.network.ConnectionDesc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Inner class of infrastructure, holds information re nodes
 *
 * @author Peter Michalak
 */
public class InfrastructureNode implements Cloneable {
    private int nodeId;
    private String state;
    private int securityLevel;
    private double dataOut;
    private double energyImpact;
    private double disk;
    private double cpu;
    private double monetaryCost;
    private double ram;
    private double batteryCapacity_mAh;
    private double batteryVoltage_V;
    private Long resourceId;
    private String resourceType;
    private List<InfrastructureResource> resources;
    private List<ConnectionDesc> connections;
    private List<String> capabilities;
    private List<Integer> downstreamNodes;

    public InfrastructureNode() { }

    public InfrastructureNode(int nodeId) {
        this.nodeId = nodeId;
    }

    public Long getResourceId() {
        return resourceId;
    }

    public void setResourceId(Long resourceId) {
        this.resourceId = resourceId;
    }

    public String getResourceType() {
        return resourceType;
    }

    public void setResourceType(String resourceType) {
        this.resourceType = resourceType;
    }

    public List<InfrastructureResource> getResources() {
        return resources;
    }

    public void setResources(List<InfrastructureResource> resources) {
        this.resources = resources;
    }

    public List<ConnectionDesc> getConnections() {
        return connections;
    }

    public void setConnections(List<ConnectionDesc> connections) {
        this.connections = connections;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public double getBatteryCapacity_mAh() {
        return batteryCapacity_mAh;
    }

    public void setBatteryCapacity_mAh(double batteryCapacity_mAh) {
        this.batteryCapacity_mAh = batteryCapacity_mAh;
    }

    public double getBatteryVoltage_V() {
        return batteryVoltage_V;
    }

    public void setBatteryVoltage_V(double batteryVoltage_V) {
        this.batteryVoltage_V = batteryVoltage_V;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public List<String> getCapabilities() {
        return capabilities;
    }

    public void setCapabilities(List<String> capabilities) {
        this.capabilities = capabilities;
    }

    public void populate(ResultSet rs) throws SQLException {
        nodeId = rs.getInt("id");
        securityLevel = Integer.parseInt(rs.getString("n.securityLevel"));
        dataOut = getDoubleFromRS(rs.getString("n.dataOut"));
        energyImpact = getDoubleFromRS(rs.getString("n.energyImpact"));
        disk = getDoubleFromRS(rs.getString("n.disk"));
        resourceId = Long.parseLong(rs.getString("n.resourceId"));
        cpu = getDoubleFromRS(rs.getString("n.cpu"));
        monetaryCost = getDoubleFromRS(rs.getString("n.monetaryCost"));
        resourceType = rs.getString("n.resourceType");
        ram = getDoubleFromRS(rs.getString("n.ram"));
        capabilities = parseNodeCapabilities(rs.getString("n.capabilities"));
    }

    /**
     * Converts format CSV capabilities into list.
     */
    private List<String> parseNodeCapabilities(String capabilities) {
        String[] capList = capabilities.split(",");
        List<String> out = new ArrayList<>();
        for (String subString : capList) {
            out.add(subString);
        }
        return out;
    }

    private double getDoubleFromRS(Object value) {
        if (value != null) {
            if (value.getClass().equals(Double.class)) {
                return (Double) value;
            } else if (value.getClass().equals(Integer.class)) {
                return (Integer) value;
            } else if (value.getClass().equals(Long.class)) {
                return (Long) value;
            }
        }
        return -1;
    }

    public void setDownstreamNodes(List<Integer> downstreamNodes) {
        this.downstreamNodes = downstreamNodes;
    }

    public List<Integer> getDownstreamNodes() {
        return downstreamNodes;
    }

    @Override
    public String toString() {
        return String.format("node id: %d, downstream nodes: %s", nodeId, downstreamNodes);
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /**
     * Determines whther the node is capable of running given operator.
     */
    public boolean canRun(String type, String operator) {
       // loop through capabilities
        for (String capability : capabilities) {
            // capabilities are currently stored as type:operator tuples e.g. "RelationalOpExpression:="
            // with limited support for regex
            String[] capDef = capability.split(":");
            // the type must match
            if (capDef[0].equals(type)) {
                // the operator can be a wildcard
                if (capDef[1].equals(operator) || (capDef[1].equals("*")))
                    return true;
            }
        }
        return false;
    }
}
