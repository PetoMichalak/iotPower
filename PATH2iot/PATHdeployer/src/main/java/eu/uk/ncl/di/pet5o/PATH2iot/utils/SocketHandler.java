package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import eu.uk.ncl.di.pet5o.PATH2iot.deployment.DeploymentHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketHandler implements Runnable {

    private static Logger logger = LogManager.getLogger(SocketHandler.class);
    private static int port;
    private boolean isRunning;
    private DeploymentHandler deploymentHandler;

    public SocketHandler(int port, DeploymentHandler deploymentHandler) {
        this.port = port;
        this.deploymentHandler = deploymentHandler;
    }

    public void run() {
        logger.debug(String.format("Server socket %d initialising...", port));
        int clientNumber = 0;
        try {
            ServerSocket listener = new ServerSocket(port);
            try {
                isRunning = true;
                while (isRunning) {
                    logger.info("Activating listener.");
                    new SocketHelper(listener.accept(), clientNumber++).start();
                    logger.debug("A listener is active.");
                    if (Thread.interrupted())
                        isRunning = false;
                }
            } finally {
                listener.close();
                logger.debug("Connection closed: " + clientNumber);
            }
        } catch (Exception e) {
            logger.error("Error when activating BLE socket connection: " + e.getMessage());
        }
    }

    private class SocketHelper extends Thread {
        private Socket socket;
        private int clientNumber;
        public SocketHelper(Socket socket, int clientNumber) {
            this.socket = socket;
            this.clientNumber = clientNumber;
            logger.debug(String.format("New connection: %s; with client %s;", socket, clientNumber));
        }

        public void run() {
            try {
                BufferedReader in = new BufferedReader((
                        new InputStreamReader(socket.getInputStream())));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

                out.println("READY");
                while (true) {
                    String input = in.readLine();
                    if (input == null || input.contains("EOF")) {
                        break;
                    }
                    logger.info("Received: " + input);
                    deploymentHandler.deploy(input);
                }
            } catch (IOException e) {
                logger.error("Error reading from socket: " + e.getMessage());
            } finally {
                try {
                    socket.close();
                } catch (IOException e) {
                    logger.error("Error closing a socket: " + e.getMessage());
                }
            }
        }
    }

}
