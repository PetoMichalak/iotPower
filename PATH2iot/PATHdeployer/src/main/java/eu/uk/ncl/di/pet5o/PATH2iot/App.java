package eu.uk.ncl.di.pet5o.PATH2iot;

import eu.uk.ncl.di.pet5o.PATH2iot.deployment.DeploymentHandler;
import eu.uk.ncl.di.pet5o.PATH2iot.deployment.ZooHandler;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.InputHandler;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.SocketHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * PATHdeployer - a module of PATH2iot system that enacts the deployement of IoT and clouds components.
 *
 * @author Peter Michalak
 */
public class App 
{
    private static Logger logger = LogManager.getLogger(App.class);

    private static InputHandler inputHandler;
    private static SocketHandler socketHandler;
    private static DeploymentHandler deploymentHandler;
    private static ZooHandler zooHandler;

    public static void main( String[] args )
    {
        logger.info("It is beginning, is it not?");

        // load a config
        inputHandler = new InputHandler(args);

        // init zookeeper handler
        try {
            zooHandler = new ZooHandler(inputHandler.getZooIp(), inputHandler.getZooPort(), inputHandler.getZooAppNode());
        } catch (IOException e) {
            logger.error("Exception during ZooKeeper server connection:" + e.getMessage());
        }

        // instantiate the deployment agent
        deploymentHandler = new DeploymentHandler(inputHandler.getInfrastructureDescription(), zooHandler, inputHandler);

        // start the server - listen for the input
        logger.debug(String.format("A connection to optimiser will be opened at port: %d.",
                inputHandler.getOptimiserPort()));
        socketHandler = new SocketHandler(inputHandler.getOptimiserPort(), deploymentHandler);
        Thread tSocket = new Thread(socketHandler);
        tSocket.run();

        logger.info("It is done.");
    }
}
