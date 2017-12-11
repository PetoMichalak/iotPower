package eu.uk.ncl.di.pet5o.PATH2iot.compile.output;

/**
 * Created by peto on 19/12/2016.
 */
public class QueueProperty {
    private String IP;
    private int port;
    private String queue;
    private String type;

    public QueueProperty(String IP, int port, String queue, String type) {
        this.IP = IP;
        this.port = port;
        this.queue = queue;
        this.type = type;
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

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
