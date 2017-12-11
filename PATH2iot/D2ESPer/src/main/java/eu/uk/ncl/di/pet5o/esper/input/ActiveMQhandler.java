package eu.uk.ncl.di.pet5o.esper.input;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.ActiveMQMessageProducer;
import org.apache.activemq.command.ActiveMQBytesMessage;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Created by peto on 18/12/2016.
 */
public class ActiveMQhandler {

    private static Logger logger = LogManager.getLogger(ActiveMQhandler.class);

    private String IP;
    private int port;
    private String queue;

    private ActiveMQConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private Destination destination;
    private ActiveMQMessageProducer producer;
    private ActiveMQMessageConsumer consumer;

    public ActiveMQhandler(String IP, int port, String queue) {
        this.IP = IP;
        this.port = port;
        this.queue = queue;

        try {
            // Create a ConnectionFactory
            connectionFactory = new ActiveMQConnectionFactory("tcp://" + IP + ":" + port);
            // Create a Connection
            connection = connectionFactory.createConnection();
            connection.start();
            // Create a Session
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            // Create the destination (Topic or Queue)
            destination = session.createQueue(queue);
            // Create a MessageProducer from the Session to the Topic or Queue
            producer = (ActiveMQMessageProducer) session.createProducer(destination);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            // Create a MessageConsumer from the Session to the Topic or Queue
            consumer = (ActiveMQMessageConsumer) session.createConsumer(destination);
        } catch (Exception e) {
            logger.error("Error establishing connection: " + e.getMessage());
        }
    }

    public void sendMessage(String msg) {
        try {
            // Create a messages
            TextMessage message = session.createTextMessage(msg);

            // Tell the producer to send the message
            logger.debug("("+queue+") Sent message: "+ message.hashCode() + " : " + Thread.currentThread().getName() +
                    "; msg: " + msg);
            if (connection != null)
            	producer.send(message);
        }
        catch (Exception e) {
            logger.error("Error sending the message: " + e);
            e.printStackTrace();
        }
    }

    public String getMessage() {
        try {
            // Wait for a message
        	Message message = null;
        	if (connection != null)
        		message = consumer.receive(1000);

            if (message instanceof TextMessage) {
                TextMessage textMessage = (TextMessage) message;
                logger.debug("Msg received ["+queue+"]: " + textMessage.getText());
                return textMessage.getText();
            } else if (message instanceof ActiveMQBytesMessage) {
                ActiveMQBytesMessage bytesMessage = (ActiveMQBytesMessage) message;
                logger.debug("Msg received.");
                return new String(bytesMessage.getContent().getData());
            } else {
                return null;
            }
        } catch(Exception e) {
            logger.error("Failed to receive a message: " + e.getMessage());
        }
        return null;
    }

    public void close() {
        try {
            // Clean up
        	consumer.close();
        	//producer.close();
            //session.close();
            connection.stop();;
            connection = null;
        } catch (Exception e) {
            logger.error("Error closing the connection: " + e.getMessage());
        }
    }

    public String getIP() {
        return IP;
    }

    public void setIP(String IP) {
        this.IP = IP;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }
}
