package eu.uk.ncl.di.pet5o.PATH2iot.input.network;

/**
 * GSON class for message bus information.
 *
 * @author Peter Michalak
 */
public class MessageBus {
    private String IP;
    private int port;
    private String type;
    private String username;
    private String pass;

    public MessageBus() {
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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }
}
