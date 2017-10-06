package eu.uk.ncl.di.pet5o.PATH2iot.operator;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by peto on 21/03/2017.
 *
 * A dataholder for a project node.
 */
public class ProjectNode {
    private int nodeId;
    private String name;
    private String asName;
    private String streamDestination;
    private String streamOrigin;
    private String type;

    public ProjectNode(int nodeId) {
        this.nodeId = nodeId;
    }

    public ProjectNode(int nodeId, String name, String asName, String streamDestination, String streamOrigin, String type) {
        this.nodeId = nodeId;
        this.name = name;
        this.asName = asName;
        this.streamDestination = streamDestination;
        this.streamOrigin = streamOrigin;
        this.type = type;
    }

    public void populate(ResultSet rs) throws SQLException {
        nodeId = rs.getInt("id");
        name = rs.getString("n.name");
        asName = rs.getString("n.asName");
        streamOrigin = rs.getString("n.streamOrigin");
        streamDestination = rs.getString("n.streamDestination");
        type = rs.getString("n.type");
    }

    public int getNodeId() {
        return nodeId;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAsName() {
        return asName;
    }

    public void setAsName(String asName) {
        this.asName = asName;
    }

    public String getStreamDestination() {
        return streamDestination;
    }

    public void setStreamDestination(String streamDestination) {
        this.streamDestination = streamDestination;
    }

    public String getStreamOrigin() {
        return streamOrigin;
    }

    public void setStreamOrigin(String streamOrigin) {
        this.streamOrigin = streamOrigin;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}

