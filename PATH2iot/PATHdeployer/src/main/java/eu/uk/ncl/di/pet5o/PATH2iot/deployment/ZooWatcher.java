package eu.uk.ncl.di.pet5o.PATH2iot.deployment;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Created by peto on 22-3-2017
 *
 * Shell implementation for a watcher,
 * more interesting things could be done here - watch for configuration changes, node failure,...
 */
public class ZooWatcher implements Watcher, Runnable{
    public ZooWatcher() {}
    private static Logger logger = LogManager.getLogger(ZooWatcher.class);

    public void process(WatchedEvent watchedEvent) {
        logger.debug("Something to watch for: " + watchedEvent);
    }

    @Override
    public void run() {
        while(true) {
            try {
                wait();
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }
}
