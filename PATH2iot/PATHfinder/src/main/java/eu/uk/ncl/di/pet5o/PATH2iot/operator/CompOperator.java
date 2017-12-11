package eu.uk.ncl.di.pet5o.PATH2iot.operator;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Created by peto on 24/02/2017.
 *
 * Internal representation of computation
 */
public class CompOperator {
    private String type;
    private String streamOrigin;
    private String streamDestination;
    private String name;
    private String operator;
    private double generationRatio;
    private double selectivityRatio;
    private double dataOut;
    private int nodeId;
    private ArrayList<Integer> downstreamOpIds;

    public CompOperator() {
        downstreamOpIds = new ArrayList<>();
    }

    public CompOperator(int nodeId) {
        this.nodeId = nodeId;
        downstreamOpIds = new ArrayList<>();
    }

    public CompOperator getCopy() {
        CompOperator tempOp = new CompOperator();
        tempOp.setType(type);
        tempOp.setStreamOrigin(streamOrigin);
        tempOp.setStreamDestination(streamDestination);
        tempOp.setName(name);
        tempOp.setOperator(operator);
        tempOp.setGenerationRatio(generationRatio);
        tempOp.setSelectivityRatio(selectivityRatio);
        tempOp.setDataOut(dataOut);
        tempOp.setNodeId(nodeId);
        for (Integer downstreamOpId : downstreamOpIds) {
            tempOp.addDownstreamOp(downstreamOpId);
        }
        return tempOp;
    }

    public CompOperator(int nodeId, String type, String streamOrigin, String streamDestination) {
        this.nodeId = nodeId;
        this.type = type;
        this.streamOrigin = streamOrigin;
        this.streamDestination = streamDestination;
        downstreamOpIds = new ArrayList<>();
    }

    /**
     * Process the result set from neo4j query to populate this instance of CompOperator object.
     * * nodeId
     * * selectivityRatio
     * * streamOrigin
     * * name
     * * streamDestination
     * * type
     * * generationRatio
     * * operator
     */
    public void populate(ResultSet rs) throws SQLException {
        nodeId = rs.getInt("id");
        selectivityRatio = getDoubleFromRS(rs.getObject("n.selectivityRatio"));
        streamOrigin = rs.getString("n.streamOrigin");
        name = rs.getString("n.name");
        streamDestination = rs.getString("n.streamDestination");
        type = rs.getString("n.type");
        generationRatio = getDoubleFromRS(rs.getObject("n.generationRatio"));
        operator = rs.getString("n.operator");
    }

    private double getDoubleFromRS(Object value) {
        if (value != null) {
            if (value.getClass().equals(Double.class)) {
                return (double) value;
            } else if (value.getClass().equals(Integer.class)) {
                return (Integer) value;
            } else if (value.getClass().equals(Long.class)) {
                return (Long) value;
            }
        }
        return -1;
    }

    @Override
    public String toString() {
        return String.format("opid: %d, downstream: %s", nodeId, downstreamOpIds);
    }

    public void addDownstreamOp(int nodeId) {
        downstreamOpIds.add(nodeId);
    }

    public ArrayList<Integer> getDownstreamOpIds() {
        return downstreamOpIds;
    }

    public void setDownstreamOpIds(ArrayList<Integer> downstreamOpIds) {
        this.downstreamOpIds = downstreamOpIds;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public String getStreamOrigin() {
        return streamOrigin;
    }

    public void setStreamOrigin(String streamOrigin) {
        this.streamOrigin = streamOrigin;
    }

    public String getStreamDestination() {
        return streamDestination;
    }

    public void setStreamDestination(String streamDestination) {
        this.streamDestination = streamDestination;
    }

    public double getGenerationRatio() {
        return generationRatio;
    }

    public void setGenerationRatio(double generationRatio) {
        this.generationRatio = generationRatio;
    }

    public double getSelectivityRatio() {
        return selectivityRatio;
    }

    public void setSelectivityRatio(double selectivityRatio) {
        this.selectivityRatio = selectivityRatio;
    }

    public double getDataOut() {
        return dataOut;
    }

    public void setDataOut(double dataOut) {
        this.dataOut = dataOut;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public void removeDownstreamOp(int nodeId) {
//        downstreamOpIds.remove(nodeId);
        ArrayList<Integer> tempDownIds = new ArrayList<>();
        for (Integer downstreamOpId : downstreamOpIds) {
            if (downstreamOpId != nodeId)
                tempDownIds.add(downstreamOpId);
        }
        downstreamOpIds = tempDownIds;
    }

    /**
     * Removes all downstream links from the operator.
     */
    public void clearDownstreamOps() {
        downstreamOpIds = new ArrayList<>();
    }

    /**
     * Compilies a string describing the operator requirement.
     * This is either a UDF and its name, or an Esper expression type with operator.
     */
    public String getRequirements() {
        return String.format("%s:%s", type, operator);
    }

    /**
     * Tests whether this is only a placeholder - no id or other properties were set
     */
    public boolean isShell() {
        if ((nodeId == 0) && (downstreamOpIds.size() == 0)) {
            return true;
        }
        return false;
    }

    /**
     * Replaces the downstream operator oldOpId -> newOpId
     */
    public void replaceDownstreamOp(int oldOpId, int newOpId) {
        // replace the operator
        if(downstreamOpIds.contains(oldOpId)) {
            downstreamOpIds.remove(new Integer(oldOpId));
            downstreamOpIds.add(newOpId);
        }
    }
}

