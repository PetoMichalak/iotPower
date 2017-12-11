package eu.uk.ncl.di.pet5o.esper.util;

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
 * v0.0.2.170322
 */
public class ZooHandler {

    private static Logger logger = LogManager.getLogger(ZooHandler.class);

    private String IP;
    private int port;
    private String root_znode;
    private String current_znode;

    private ZooKeeper zk;
    private ZooWatcher zooWatcher;

    public ZooHandler(String IP, int port, String root_znode) throws IOException {
        this.IP = IP;
        this.port = port;
        this.root_znode = root_znode;

        // establish connection
        zooWatcher = new ZooWatcher();
        zk = new ZooKeeper(IP + ":" + port, 3000, zooWatcher);
    }

    public void createZnode(String znode) {
        try {
            current_znode = root_znode + "/" + znode;
            zk.create(current_znode, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException | InterruptedException e) {
            logger.error(e.getMessage());
        }
        logger.debug("Created " + znode + " under " + root_znode);
    }

    public List<String> getCurrentZnodes() {
        logger.debug("Current nodes under: " + current_znode);
        List<String> nodes = new ArrayList<>();
        try {
            for (String node : zk.getChildren(current_znode, false)) {
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
        logger.debug("Getting config from ZooKeeper ");
        try {
            data = zk.getData(current_znode, null, null);
            if (data!=null) {
                logger.debug("Received: " + data.length + " B.");
            } else {
                System.out.print(".");
            }
        } catch (KeeperException | InterruptedException e) {
            logger.error(e.getMessage());
        }
        return data;
    }

    public void setCurrentData(String data) {
        try {
            zk.setData(current_znode, data.getBytes(), -1);
        } catch (KeeperException | InterruptedException e) {
            logger.error("Cannot set data to node: " + current_znode + "; e:" + e.getMessage());
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

    public String getRoot_znode() {
        return root_znode;
    }

    public void setRoot_znode(String root_znode) {
        this.root_znode = root_znode;
    }

	public ZooKeeper getZk() {
		return zk;
	}
}
