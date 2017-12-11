package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureNode;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.CompOperator;
import eu.uk.ncl.di.pet5o.PATH2iot.operator.ProjectNode;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by peto on 01/01/2017.
 *
 * Neo4j handler.
 * v0.0.4
 */
public class NeoHandler {

    private static Logger logger = LogManager.getLogger(NeoHandler.class);
    private String address;
    private Connection con;

    /**
     * Establish bolt connection to the Neo4j database.
     */
    public NeoHandler(String address, String neoUser, String neoPass) {
        this.address = address;
        try {
            // Connect
            con = DriverManager.getConnection("jdbc:neo4j:bolt://" + address, neoUser, neoPass);
            logger.debug("NEO4J connection established.");
        } catch (SQLException e) {
            logger.error("Problem with establishing connection: " + e.getMessage());
        }

        // always start with a clean database
        cleanDb();
    }

    int createNode(String nodeType, Map<String, Object> pairs) {
        int nodeId = -1;
        try (Statement stmt = con.createStatement()) {
            String properties = "";
            for (String s : pairs.keySet()) {
                if (pairs.get(s).getClass().equals(String.class)) {
                    properties += String.format("%s:\"%s\",", s, pairs.get(s));
                } else {
                    properties += String.format("%s:%s,", s, pairs.get(s));
                }
            }
            if (properties.length() > 0) {
                properties = properties.substring(0, properties.length() - 1);
            }
            String query = String.format("CREATE (n:%s {%s}) RETURN ID(n)", nodeType, properties);

            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                // should really have only one result
                nodeId = rs.getInt("ID(n)");
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + e.getMessage());
        }
        return nodeId;
    }

    public void query(String query, String item) {
        // Querying
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            logger.info("Results (" + query + "):");
            while (rs.next()) {
                logger.info("- " + rs.getString(1));
                logger.info(rs.getString("id"));
                logger.info(rs.getString("labels"));
                logger.info(rs.getString("op"));
            }
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
    }

    private boolean insert(String query) {
        boolean outcome = false;
        try (Statement stmt = con.createStatement()) {
            outcome = stmt.execute(query);
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return outcome;
    }

    /**
     * Builds a single node in Neo4j
     *
     * @param query
     * @return nodeId; -1 if unsuccessful
     */
    public int createNode(String query) {
        int nodeId = -1;
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                // should really have only one result
                nodeId = rs.getInt("ID(n)");
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return nodeId;
    }

    public void close() {
        try {
            con.close();
        } catch (SQLException e) {
            logger.error("Problem closing the connection: " + e.getMessage());
        }
    }

    /**
     * Given the nodeId returns variable property as String
     *
     * @param nodeId
     * @param prop   property
     * @return
     */
    public String getVariableById(Integer nodeId, String prop) {
        String out = "";
        String query = String.format("match (n) where ID(n)=%d return n.%s", nodeId, prop);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                // should really have only one result
                out = rs.getString(String.format("n.%s", prop));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Given a name returns node ID
     *
     * @param source
     * @return node id; -1 if failed
     */
    public int getNodeIdByName(String source) {
        // potentionally dangerous!
        int nodeId = -1;
        String query = String.format("match (n) where n.name=\"%s\" return ID(n)", source);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                // should really have only one result
                nodeId = rs.getInt("ID(n)");
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return nodeId;
    }

    /**
     * Creates given relationship in between two nodes.
     * // TODO node ids should really be of Long type
     *
     * @param nodeId
     * @param nodeId2
     * @param rel
     */
    public boolean createRel(int nodeId, String rel, int nodeId2) {
        String query = String.format("MATCH (m),(n) WHERE ID(m)=%d AND ID(n)=%d MERGE (m)-[r:%s]->(n)",
                nodeId, nodeId2, rel);
        logger.info(String.format("created rel: (%d)-[:%s]->(%d)", nodeId, rel, nodeId2));
        return insert(query);
    }

    /**
     * Creates a relationship in between any two types of nodes.
     *
     * @param node1
     * @param node1type
     * @param node2
     * @param node2type
     * @param rel
     * @return bool success
     */
    public boolean createRel(Integer node1, String node1type, int node2, String node2type, String rel) {
        String query = String.format("match (m:%s),(n:%s) WHERE ID(m)=%d AND ID(n)=%d create (m)-[r:%s]->(n)",
                node1type, node2type, node1, node2, rel);
        return insert(query);
    }

    /**
     * Removes all nodes and relationships from the database
     *
     * @return bool success
     */
    public boolean cleanDb() {
        String query = "Match (n) DETACH DELETE n";
        return insert(query);
    }

    public int addNode(String nodeType, String name, String key, String value) {
        int nodeId = -1;
        String query = String.format("CREATE (n:%s {name: \"%s\", type:\"%s\", value: \"%s\"}) RETURN ID(n)",
                nodeType, name, key, value);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                // should really have only one result
                nodeId = rs.getInt("ID(n)");
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return nodeId;
    }

    public boolean linkNodeTypes(String nodeType, String streamOrigin, String nodeType2, String streamOrigin2, String rel) {
        String query = String.format("MATCH (n:%s {streamOrigin:\"%s\"}), (m:%s {streamOrigin:\"%s\"}) " +
                "CREATE (n)-[:%s]->(m)", nodeType, streamOrigin, nodeType2, streamOrigin2, rel);
        logger.info(String.format("linked all: (%s)-[:%s]->(%s) of %s", nodeType, rel, nodeType2, streamOrigin));
        return insert(query);
    }

    boolean linkNodeToSource(int nodeId, String nodeType, String sourceType, String streamOrigin,
                             String propertyKey, String propertyName, String rel) {
        String query = String.format("MATCH (n:%s), (m:%s {streamDestination:\"%s\", %s:\"%s\"}) " +
                        "WHERE ID(n)=%d CREATE (m)-[:%s]->(n)", nodeType, sourceType, streamOrigin, propertyKey,
                propertyName, nodeId, rel);
        logger.info("Executed: " + query);
        return insert(query);
    }

    boolean linkNodeToSource(int nodeId, String nodeType, String sourceType, String streamOrigin, String rel) {
        String query = String.format("MATCH (n:%s), (m:%s {streamDestination:\"%s\"}) " +
                        "WHERE ID(n)=%d CREATE (m)-[:%s]->(n)", nodeType, sourceType, streamOrigin,
                nodeId, rel);
        logger.info("Executed: " + query);
        return insert(query);
    }


    public boolean linkNodeToSource(int childNode, String nodeType, String sourceType, String streamOrigin,
                                    String streamDest, String rel) {
        String query = String.format("MATCH (n:%s), (m:%s {streamDestination:\"%s\", streamOrigin:\"%s\"}) " +
                        "WHERE ID(n)=%d CREATE (m)-[:%s]->(n)",
                nodeType, sourceType, streamDest, streamOrigin, childNode, rel);
        logger.info("Executed: " + query);
        return insert(query);
    }

    public boolean linkNodeToChild(int nodeId, String nodeType, String childType, String streamOrigin,
                                   String streamDestination, String rel) {
        String query = String.format("MATCH (n:%s), (m:%s {streamOrigin:\"%s\", streamDestination:\"%s\"}) " +
                        "WHERE ID(n)=%d CREATE (n)-[:%s]->(m)",
                nodeType, childType, streamOrigin, streamDestination, nodeId, rel);
        logger.info("Executed: " + query);
        return insert(query);

    }

    public boolean linkNodeByProperty(String node1type, String node1value, String node2type, String node2value,
                                      String node1prop, String node2prop, String rel) {
        String query = String.format("MATCH (n:%s {%s:\"%s\"}), (m:%s {%s:\"%s\"}) CREATE (n)-[:%s]->(m)",
                node1type, node1prop, node1value, node2type, node2prop, node2value, rel);
        logger.info("Executed: " + query);
        return insert(query);
    }

    /**
     * ToDo this should go
     */
    @Deprecated
    public ArrayList<Integer> findNodesWithProperty(String type, String propertyName, String propertyValue) {
        ArrayList<Integer> out = new ArrayList<>();
        String query = String.format("MATCH (n:%s {%s:\"%s\"}) RETURN n.resourceId AS out",
                type, propertyName, propertyValue);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                // ToDo not sure why this is string and with extra parenthesis
                String resourceId = rs.getString("out");
                out.add(Integer.parseInt(resourceId.replaceAll("\"", "")));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Returns a list of node Ids from verteces with type label, propName:propValue
     */
    public ArrayList<Integer> findNodesWithProperty2(String type, String propertyName, Object propertyValue) {
        ArrayList<Integer> out = new ArrayList<>();
        String query = "";
        if (propertyName instanceof String) {
            query = String.format("MATCH (n:%s {%s:\"%s\"}) RETURN ID(n) AS out",
                    type, propertyName, propertyValue);
        } else {
            query = String.format("MATCH (n:%s {%s:%s}) RETURN ID(n) AS out",
                    type, propertyName, propertyValue);
        }
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getInt("out"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    public ArrayList<Integer> findNodesWithoutIncomingRelationship(String nodeType, String rel) {
        ArrayList<Integer> out = new ArrayList<>();
        String query = String.format("MATCH (n:%s) WHERE NOT (n)<-[:%s]-() RETURN ID(n) AS out", nodeType, rel);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getInt("out"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Finds next downstream nodes of given label2 from nodeId:label1 within maxDistance over rel relationship.
     * MATCH (n:COMP),(m:COMP) WHERE ID(n)=6526 AND (n)-[:STREAMS*..3]->(m) RETURN ID(m) AS out
     */
    public ArrayList<Integer> findDownstreamNodes(Integer nodeId, String nodeType, String node2type, String rel, int maxDistance) {
        ArrayList<Integer> out = new ArrayList<>();
        // traverse distance from 0 up to 5
        for (int i = 1; i <= maxDistance; i++) {
            String query = String.format("MATCH (n:%s), (m:%s) WHERE ID(n)=%d AND (n)-[:%s*%d]->(m) RETURN ID(m) as out",
                    nodeType, node2type, nodeId, rel, i);
            try (Statement stmt = con.createStatement()) {
                ResultSet rs = stmt.executeQuery(query);
                while (rs.next()) {
                    out.add(rs.getInt("out"));
                }
                logger.debug("Query: " + query + " - executed.");
            } catch (SQLException e) {
                logger.error("Error executing query: " + query + "; error: " + e.getMessage());
            }
            // single result is enough
            if (out.size() > 0) {
                break;
            }
        }
        return out;
    }

    /**
     * Finds all downstream nodes of given label2 from nodeId:label1
     * MATCH (n:COMP),(m:COMP) WHERE ID(n)=6526 AND (n)-[:STREAMS*]->(m) RETURN ID(m) AS out
     */
    public ArrayList<Integer> findAllDownstreamNodes(Integer nodeId, String nodeType, String node2type, String rel) {
        ArrayList<Integer> out = new ArrayList<>();
        // traverse distance from 0 up to 5
        String query = String.format("MATCH (n:%s), (m:%s) WHERE ID(n)=%d AND (n)-[:%s*]->(m) RETURN ID(m) as out",
                nodeType, node2type, nodeId, rel);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getInt("out"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Finds next upstream nodes of given label2 from nodeId:label1 within maxDistance over rel relationship.
     * MATCH (n:COMP),(m:COMP) WHERE ID(n)=6526 AND (n)<-[:STREAMS*..3]-(m) RETURN ID(m) AS out
     */
    public ArrayList<Integer> findUpstreamNodes(int nodeId, String label1, String label2, String rel, int maxDistance) {
        ArrayList<Integer> out = new ArrayList<>();
        // traverse distance from 0 up to 5
        for (int i = 1; i <= maxDistance; i++) {
            String query = String.format("MATCH (n:%s), (m:%s) WHERE ID(n)=%d AND (n)<-[:%s*%d]-(m) RETURN ID(m) as out",
                    label1, label2, nodeId, rel, i);
            try (Statement stmt = con.createStatement()) {
                ResultSet rs = stmt.executeQuery(query);
                while (rs.next()) {
                    out.add(rs.getInt("out"));
                }
                logger.debug("Query: " + query + " - executed.");
            } catch (SQLException e) {
                logger.error("Error executing query: " + query + "; error: " + e.getMessage());
            }
            // single result is enough
            if (out.size() > 0) {
                break;
            }
        }
        return out;
    }

    /**
     * Finds 'closest' downstream nodes with specific property.
     */
    public ArrayList<Integer> findDownstreamNodesWithProperty(Integer nodeId, String nodeType, String node2type,
                                                              String propName, Object propValue,
                                                              String rel, int maxDistance) {
        ArrayList<Integer> out = new ArrayList<>();
        // traverse distance from 0 up to 5
        for (int i = 1; i < maxDistance; i++) {
            String query = String.format("MATCH (n:%s), (m:%s {%s:%s}) WHERE ID(n)=%d AND (n)-[:%s*%d]->(m) " +
                    "RETURN ID(m) as out", nodeType, node2type, propName, propValue, nodeId, rel, i);
            try (Statement stmt = con.createStatement()) {
                ResultSet rs = stmt.executeQuery(query);
                while (rs.next()) {
                    out.add(rs.getInt("out"));
                }
                logger.debug("Query: " + query + " - executed.");
            } catch (SQLException e) {
                logger.error("Error executing query: " + query + "; error: " + e.getMessage());
            }
            // single result is enough
            if (out.size() > 0) {
                break;
            }
        }
        return out;
    }

    /**
     * ToDo this should ideally go (returning Object instead)
     */
    @Deprecated
    public String getNodeProperty(int compNodeId, String type, String property) {
        String out = "";
        String query = String.format("MATCH (n:%s) WHERE ID(n)=%d RETURN n.%s AS out", type, compNodeId, property);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                // should really be just one
                out = rs.getString("out");
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Returns a queried property from given nodeId:label vertex.
     *
     * @param nodeId
     * @param label
     * @param propertyName
     * @return object
     */
    public Object getNodeProperty2(int nodeId, String label, String propertyName) {
        Object out = "";
        String query = String.format("MATCH (n:%s) WHERE ID(n)=%d RETURN n.%s AS out", label, nodeId, propertyName);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                // should really be just one
                out = rs.getObject("out");
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * e.g. MATCH (n:NODE) WHERE (n)<-[:PLACED]-() RETURN n.resourceId AS out
     * @return list of resourceIds
     */
    public ArrayList<Integer> findNodesWithIncomingRelationship(String type, String rel) {
        ArrayList<Integer> out = new ArrayList<>();
        String query = String.format("MATCH (n:%s) WHERE (n)<-[:%s]-() RETURN ID(n) AS out", type, rel);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getInt("out"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Searches for vertices of given type that have outgoing edge as specified
     *
     * @param type label
     * @param rel  outgoing relationship/edge
     * @return list of node ids
     */
    public ArrayList<Integer> findSourcesOfRelationship(String type, String rel) {
        ArrayList<Integer> out = new ArrayList<>();
        String query = String.format("MATCH (n:%s) WHERE (n)-[:%s]->() RETURN ID(n) AS out", type, rel);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getInt("out"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Get all node Ids of type2 label connected to nodeId:type over rel.
     */
    public ArrayList<Integer> getConnectedNodes(int nodeId, String type, String type2, String rel) {
        ArrayList<Integer> out = new ArrayList<>();
        String query = String.format("MATCH (n:%s), (m:%s) WHERE ID(n)=%d AND (n)<-[:%s]-(m) RETURN ID(m) AS out",
                type, type2, nodeId, rel);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getInt("out"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * MATCH (n:NODE {resourceId:"115001"}) RETURN ID(n) AS out
     */
    public int getNodeIdByResourceId(int resourceId, String type) {
        int nodeId = -1;
        String query = String.format("MATCH (n:%s {resourceId:\"%d\"}) RETURN ID(n) as out", type, resourceId);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                nodeId = rs.getInt("out");
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return nodeId;
    }

    /**
     * MATCH (n:COMP), (m:NODE) WHERE ID(n)=2046 AND ID(m)=2030 CREATE (n)-[:PLACED]->(m)
     */
    public boolean linkNodes(int node1id, String node1type, int node2id, String node2type, String rel) {
        String query = String.format("MATCH (n:%s), (m:%s) WHERE ID(n)=%d AND ID(m)=%d CREATE (n)-[:%s]->(m)",
                node1type, node2type, node1id, node2id, rel);
        logger.info("Executed: " + query);
        return insert(query);
    }

    /**
     * MATCH (n:NODE),(m:NODE) WHERE ID(n)=4735 AND ID(m)=4736 CREATE (n)-[:CONNECTS {bandwidth:2,monetaryCost:0.0}]->(m)
     */
    boolean linkNodes(int nodeId1, String label1, int nodeId2, String label2, Map<String, Object> props,
                      String rel) {
        String properties = "";
        for (String s : props.keySet()) {
            properties += String.format("%s:\"%s\",", s, props.get(s));
        }
        if (properties.length() > 0) {
            properties = properties.substring(0, properties.length() - 1);
        }
        String query = String.format("MATCH (n:%s),(m:%s) WHERE ID(n)=%d AND ID(m)=%d CREATE (n)-[:%s {%s}]->(m)",
                label1, label2, nodeId1, nodeId2, rel, properties);

        logger.info("Executed: " + query);
        return insert(query);
    }

    /**
     * For given nodeId, return the IDs of host nodes where it was placed.
     * e.g. MATCH (n:COMP),(m:NODE) WHERE ID(n)=3014 AND (n)-[:PLACED]->(m) RETURN ID(m) AS out
     */
    public ArrayList<Integer> getAllHosts(Integer nodeId, String type, String type2, String rel) {
        ArrayList<Integer> out = new ArrayList<>();
        String query = String.format("MATCH (n:%s),(m:%s) WHERE ID(n)=%d AND (n)-[:%s]->(m) RETURN ID(m) AS out",
                type, type2, nodeId, rel);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getInt("out"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Create a new relationship to all nodes connected to the source node.
     * MATCH (n:NODE), (m:NODE) WHERE ID(n) = 3203 AND (n)-[:CONNECTS]->(m) CREATE UNIQUE (n)-[:AccelEvents]->(m) RETURN n,m
     *
     * @param nodeId      source node
     * @param nodeType    source node type
     * @param relExisting existing relationship between source and other nodes
     * @param node2type   label of the other nodes
     * @param relNew      new connection to be created (if doesn't exist)
     */
    public boolean createRelToAllConnectedNodes(int nodeId, String nodeType, String relExisting, String node2type,
                                                String relNew) {
        String query = String.format("MATCH (n:%s), (m:%s) WHERE ID(n)=%d AND (n)-[:%s]->(m) " +
                        "CREATE UNIQUE (n)-[:%s]->(m) RETURN n,m",
                nodeType, node2type, nodeId, relExisting, relNew);
        logger.info("Executed: " + query);
        return insert(query);
    }

    /**
     * query to get node ids for COMP vertices which don't have PLACED relationship
     *
     * @return list of node ids which were not yet placed
     */
    public ArrayList<Integer> getNotPlacedNodes() {
        ArrayList<Integer> out = new ArrayList<>();
        String query = "MATCH (n:COMP) WHERE NOT (n)-[:PLACED]->() RETURN ID(n) AS out";
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getInt("out"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Search from the given node Id along STREAMS connections to get vertices which were not yet placed.
     */
    public ArrayList<Integer> findShortestPath(Integer nodeId, String node1label, String node2label,
                                               String traverseRel, String missingRel) {
        ArrayList<Integer> out = new ArrayList<>();
        String query = String.format("MATCH (n:%s), (m:%s), path = shortestpath((n)-[:%s*]->(m)) " +
                        "WHERE ID(n)=%d AND NOT(m)-[:%s]-() RETURN ID(m) AS out " +
                        "ORDER BY LENGTH(path) ASC LIMIT 3",
                node1label, node2label, traverseRel, nodeId, missingRel);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getInt("out"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Return a list of all ids from neo4j for given label.
     * e.g. MATCH (n:COMP) RETURN ID(n) AS out
     *
     * @param label label
     * @return list of ids
     */
    public ArrayList<Integer> getAllVertices(String label) {
        ArrayList<Integer> out = new ArrayList<>();
        String query = String.format("MATCH (n:%s) RETURN ID(n) AS out", label);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getInt("out"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Get a list of node ids for given computation.
     * e.g. MATCH (n:COMP),(m:NODE) WHERE ID(n)=4460 AND (n)-[:PLACED]->(m) RETURN ID(m) AS out
     *
     * @param nodeId comp node id
     * @param label1 label of the node
     * @param rel    relationship connecting the nodes
     * @param label2 label of the targeted node
     * @return list of ids of targetted nodes
     */
    public ArrayList<Integer> getCompPlacement(Integer nodeId, String label1, String rel, String label2) {
        ArrayList<Integer> out = new ArrayList<>();
        String query = String.format("MATCH (n:%s),(m:%s) WHERE ID(n)=%d AND (n)-[:%s]->(m) RETURN ID(m) AS out",
                label1, label2, nodeId, rel);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getInt("out"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Gets all PROJECT node id with specified stream name.
     * @param streamOrigin stream name
     */
    public List<Integer> getProjectNodesByStreamOrigin(String streamOrigin) {
        ArrayList<Integer> out = new ArrayList<>();
        String query = String.format("MATCH (n:PROJECT {streamOrigin:\"%s\"}) RETURN ID(n) AS id", streamOrigin);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getInt("id"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Adds or updates a property for a specific node
     * MATCH (n) WHERE ID(n)=4904 SET n.generationRatio=3601 RETURN n
     */
    public boolean addProperty(int nodeId, String propName, Object propValue) {
        String query;
        if (propValue instanceof String) {
            query = String.format("MATCH (n) WHERE ID(n)=%d SET n.%s=\"%s\" RETURN n", nodeId, propName, propValue);
        } else {
            query = String.format("MATCH (n) WHERE ID(n)=%d SET n.%s=%s RETURN n", nodeId, propName, propValue);
        }
        logger.debug("Executed: " + query);
        return insert(query);
    }

    /**
     * Query the neo4j database and populate the CompOperator instance.
     */
    public CompOperator getOperatorById(Integer nodeId) {
        String query = String.format("match (n:COMP) WHERE ID(n)=%d return n.selectivityRatio, n.streamOrigin, " +
                "n.name, n.streamDestination, n.type, n.generationRatio, n.operator, ID(n) as id", nodeId);
        CompOperator tempOp = new CompOperator();
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                tempOp.populate(rs);
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return tempOp;
    }

    /**
     * Query the neo4j database and populate the InfrastructureNode instance.
     */
    public InfrastructureNode getInfrastructureNodeById(Integer nodeId) {
        String query = String.format("MATCH (n:NODE) WHERE ID(n)=%d RETURN n.securityLevel, n.dataOut, n.energyImpact, " +
                "n.disk, n.resourceId, n.cpu, n.monetaryCost, n.resourceType, n.ram, n.capabilities, n.defaultNetworkFreq, ID(n) as id", nodeId);
        InfrastructureNode tempNode = new InfrastructureNode(nodeId);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                tempNode.populate(rs);
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return tempNode;
    }

    /**
     * Query the neo4j database and populate the ProjectNode instance.
     */
    public ProjectNode getProjectById(Integer nodeId) {
        String query = String.format("MATCH (n:PROJECT) WHERE ID(n)=%d RETURN ID(n) AS id, n.streamOrigin, n.asName, " +
                "n.type, n.name, n.streamDestination", nodeId);
        ProjectNode tempNode = new ProjectNode(nodeId);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                tempNode.populate(rs);
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return tempNode;
    }

    /**
     * Pulls neo4j with set of properties and returns a single value for each returned record.
     * // ToDo - pass list of values, change return type
     */
    List<String> findNodesWithProperties(String label, Map<String, Object> keyPairs, String returnField) {
        List<String> out = new ArrayList<>();
        String props = "";
        for (String key : keyPairs.keySet()) {
            if (keyPairs.get(key).getClass().equals(String.class)) {
                props += String.format("%s:\"%s\", ", key, keyPairs.get(key));
            } else {
                props += String.format("%s:%d, ", key, keyPairs.get(key));
            }
        }
        // strip the last comma
        props = props.substring(0, props.length() - 2);

        String query = String.format("MATCH (n:%s {%s}) RETURN n.%s", label, props, returnField);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getString("n."+returnField));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Pulls neo4j with set of properties and returns a single value for each returned record.
     * Matches ONLY outbound nodes
     */
    List<String> findOutboundNodesWithProperties(String label, Map<String, Object> keyPairs) {
        List<String> out = new ArrayList<>();
        String props = "";
        for (String key : keyPairs.keySet()) {
            if (keyPairs.get(key).getClass().equals(String.class)) {
                props += String.format("%s:\"%s\", ", key, keyPairs.get(key));
            } else {
                props += String.format("%s:%d, ", key, keyPairs.get(key));
            }
        }
        // strip the last comma
        props = props.substring(0, props.length() - 2);

        String query = String.format("MATCH (n:%s {%s}) WHERE ()-[]->(n) RETURN n.name, n.asName", label, props);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getString("n.name") + "," + rs.getString("n.asName"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Creates a node (label: PROJECT) in Neo4j with following properties
     * @param name original name as used to access the data
     * @param asName alias name - if not defined, same as the previous
     * @param dataType ["integer", "double", "long", "string", "bool"]
     * @param streamOrigin name of a input data stream
     * @param streamDestination name of the output data stream
     * @return id of operator from neo4j db
     */
    int createProject(String name, String asName, String dataType, String streamOrigin, String streamDestination) {
        int nodeId = -1;
        String query = String.format("CREATE (n:PROJECT {name: \"%s\", asName: \"%s\", type:\"%s\", streamOrigin: \"%s\", " +
                        "streamDestination: \"%s\"}) RETURN ID(n) AS id", name, asName, dataType, streamOrigin, streamDestination);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                // should really have only one result
                nodeId = rs.getInt("id");
            }
            logger.debug("PROJECT node created: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return nodeId;
    }

    /**
     * Creates a node (label: SINK) in Neo4j with following properties and find it source
     * @param name original name as used to access the data
     * @param dataType ["integer", "double", "long", "string", "bool"]
     * @param streamOrigin name of a input data stream
     * @param streamDestination name of the output data stream
     * @return id of operator from neo4j db
     */
    int createSink(String name, String dataType, String streamOrigin, String streamDestination) {
        int nodeId = -1;
        String query = String.format("CREATE (n:SINK {name: \"%s\", type:\"%s\", streamOrigin: \"%s\", " +
                "streamDestination: \"%s\"}) RETURN ID(n) AS id", name, dataType, streamOrigin, streamDestination);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                // should really have only one result
                nodeId = rs.getInt("id");
            }
            logger.debug("SINK node created: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }

        // find the PROJECT node and link to it
        linkNodeToSource(nodeId, "SINK", "PROJECT", streamOrigin,
                "name", name, "STREAMS");

        return nodeId;
    }

    /**
     * Creates a node (label: COMP) in Neo4j with following properties.
     * @param name name for this operator
     * @param streamOrigin name of a input data stream
     * @param streamDestination name of the output data stream
     * @param type type of the operator (e.g. name of the SODA class representing this operation)
     * @param operator the operator (*,-,+,/)
     * @param selectivityRatio <0, 1>
     * @param generationRatio <0, 1>
     * @return id of operator from neo4j db
     */
    int createComp(String name, String streamOrigin, String streamDestination, String type, String operator,
                   double selectivityRatio, double generationRatio) {
        int nodeId = -1;
        String query = String.format("CREATE (n:COMP {name: \"%s\", streamOrigin: \"%s\", streamDestination: \"%s\", " +
                "type: \"%s\", operator: \"%s\", selectivityRatio: %f, generationRatio: %f}) RETURN ID(n) AS id",
                name, streamOrigin, streamDestination, type, operator, selectivityRatio, generationRatio);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                // should really have only one result
                nodeId = rs.getInt("id");
            }
            logger.debug("COMP node created: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }

        return nodeId;
    }

    /**
     * MATCH (n:NODE), (m:NODE), (p:NODE) WHERE ID(n) = 9300 AND ID(m) = 9304 AND
     * (n)-[:CONNECTS*1..10]->(p)-[:CONNECTS*1..10]->(m) RETURN p
     */
    public ArrayList<Integer> getIntermediateNodeIds(String label, int node1Id, int node2Id,
                                                     String linkName, int maxDistance) {
        ArrayList<Integer> out = new ArrayList<>();
        String query = String.format("MATCH (n:%s), (m:%s), (p:%s) WHERE ID(n) = %d AND ID(m) = %d " +
                "AND (n)-[:%s*1..%d]->(p)-[:%s*1..%d]->(m) RETURN ID(p) as id", label, label, label,
                node1Id, node2Id,linkName, maxDistance, linkName, maxDistance);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getInt("id"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }

    /**
     * Queries Neo4j for a PROJECT node with asName and given destination and returns its id (-1 if not found).
     */
    int findSource(String asName, String sourceDestination) {
        int nodeId = -1;
        String query = String.format("MATCH (n:PROJECT {asName:\"%s\", streamDestination:\"%s\"}) RETURN ID(n) as id",
                asName, sourceDestination);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                // should really have only one result
                nodeId = rs.getInt("id");
            }
            logger.debug("Source node found: " + nodeId);
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return nodeId;
    }

    /**
     * Find all Project nodes linked to the given COMP node.
     */
    ArrayList<Integer> findProjectNodes(int nodeId) {
        ArrayList<Integer> out = new ArrayList<>();
        String query = String.format("MATCH (n:PROJECT) ,(m:COMP) " +
                        "WHERE ID(m)=%d AND (m)-[:STREAMS]->(n) " +
                        "RETURN ID(n) as id", nodeId);
        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                out.add(rs.getInt("id"));
            }
            logger.debug("Query: " + query + " - executed.");
        } catch (SQLException e) {
            logger.error("Error executing query: " + query + "; error: " + e.getMessage());
        }
        return out;
    }
}