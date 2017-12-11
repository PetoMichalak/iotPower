package eu.uk.ncl.di.pet5o.PATH2iot.deployment;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by peto on 22/03/2017.
 *
 * (very simple) connector to a zookeeper server.
 * v0.0.3.170323
 */
public class ZooHandler {

    private static Logger logger = LogManager.getLogger(ZooHandler.class);

    private String IP;
    private int port;
    private String rootZnode;
    private String currentZnode;

    private ZooKeeper zk;
    private ZooWatcher zooWatcher;

    public ZooHandler(String IP, int port, String rootZnode) throws IOException {
        this.IP = IP;
        this.port = port;
        this.rootZnode = rootZnode;

        // establish connection
        zooWatcher = new ZooWatcher();
        zk = new ZooKeeper(IP + ":" + port, 3000, zooWatcher);
    }

    public void createZnode(String znode) {
        try {
            currentZnode = rootZnode + "/" + znode;
            zk.create(currentZnode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//            zk.create(currentZnode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            logger.error(e.getMessage());
        }
        logger.debug("Created " + znode + " under " + rootZnode);
    }

    public void createZnode(String znode, CreateMode createMode) {
        try {
            currentZnode = rootZnode + "/" + znode;
            zk.create(currentZnode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
        } catch (KeeperException | InterruptedException e) {
            logger.error(e.getMessage());
        }
        logger.debug("Created " + znode + " under " + rootZnode);
    }

    public List<String> getZnodes(String path) {
        logger.debug("Current nodes under: " + path);
        List<String> nodes = new ArrayList<>();
        try {
            for (String node : zk.getChildren(path, false)) {
                nodes.add(node);
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error(e.getMessage());
        }
        return nodes;
    }

    public List<String> getCurrentZnodes() {
        logger.debug("Current nodes under: " + currentZnode);
        List<String> nodes = new ArrayList<>();
        try {
            for (String node : zk.getChildren(currentZnode, false)) {
                nodes.add(node);
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error(e.getMessage());
        }
        return nodes;
    }

    public void close() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            logger.error("Failed closing zookeeper connection: " + e.getMessage());
        }
    }

    public byte[] getCurrentData() {
        byte[] data = null;
        try {
            data = zk.getData(currentZnode, zooWatcher, null);
            if (data!=null) {
                logger.debug("Received: " + data.length + " B.");
            } else {
                logger.debug("Current znode data is null.");
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error(e.getMessage());
        }
        return data;
    }

    public void setCurrentData(String data) {
        try {
            zk.setData(currentZnode, data.getBytes(), -1);
        } catch (KeeperException | InterruptedException e) {
            logger.error("Cannot set data to node: " + currentZnode + "; e:" + e.getMessage());
        }
    }

    public String getIP() {
        return IP;
    }

    public void setIP(String IP) {
        this.IP = IP;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getRootZnode() {
        return rootZnode;
    }

    public void setRootZnode(String rootZnode) {
        this.rootZnode = rootZnode;
    }

    public String getCurrentZnode() {
        return currentZnode;
    }

    public void setCurrentZnode(String currentZnode) {
        this.currentZnode = currentZnode;
    }
}
