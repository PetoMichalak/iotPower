package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * Servess to connect to deployer and transfer the configuration files over.
 */
public class SocketClientHandler {

    private static Logger logger = LogManager.getLogger(SocketClientHandler.class);

    private String ip;
    private int port;
    private Socket socket;
    private PrintWriter out;

    public SocketClientHandler(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    /**
     * Established connection to the socket server.
     */
    public void connect() {
        // establish connection
        boolean establishingConnection = true;
        while(establishingConnection) {
            try {
                socket = new Socket(ip, port);
                out = new PrintWriter(socket.getOutputStream(), true);
                logger.debug(String.format("Connection established %s:%d", ip, port));
                establishingConnection = false;
            } catch (IOException | NullPointerException e) {
                logger.error(String.format("Waiting ... Connection to %s:%d failed. %s", ip, port, e.getMessage()));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    logger.error("Error sleeping: " + e.getMessage());
                }
            }
        }
    }

    /**
     * sends the message over the socket connection
     */
    public void send(String msg) {
        out.println(msg);
    }

    /**
     * Closes socket and output buffer.
     */
    public void close() {
        // following internal protocol, the server shuts down connection upon receiving "EOF"
        send("EOF");
        try {
            socket.close();
        } catch (IOException e) {
            logger.error("Error while closing socket connection: " + e.getMessage());
        }
        out.close();
    }

}
