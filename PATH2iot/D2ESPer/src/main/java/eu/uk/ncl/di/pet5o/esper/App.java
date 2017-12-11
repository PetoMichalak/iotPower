package eu.uk.ncl.di.pet5o.esper;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.uk.ncl.di.pet5o.esper.streamProcessing.subscribers.DynamicSub;
import eu.uk.ncl.di.pet5o.esper.streamProcessing.subscribers.StatementValidator;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import eu.uk.ncl.di.pet5o.esper.input.ActiveMQhandler;
import eu.uk.ncl.di.pet5o.esper.input.EsperDefJson;
import eu.uk.ncl.di.pet5o.esper.input.EsperStatement;
import eu.uk.ncl.di.pet5o.esper.input.EsperStream;
import eu.uk.ncl.di.pet5o.esper.input.EventProperty;
import eu.uk.ncl.di.pet5o.esper.util.MessageProcessor;
import eu.uk.ncl.di.pet5o.esper.util.ZooHandler;

/**
 * A dynamic Esper stream processor, used within PATH2iot system.
 * 
 * @author Peter Michalak
 * @author Saleh Mohamed
 */
public class App {
	// Logger
	private static Logger logger = LogManager.getLogger(App.class);

	private static long resource_id;
	private static boolean local_file;
	private static String local_epl_path;
	private static String zoo_ip;
	private static int zoo_port;
	private static String zoo_app_node;

	private static Gson gson;
	private static EsperDefJson esperDefJson;
	private static MessageProcessor msgProc;
	private static ZooHandler zooHandler;
	private static ZooKeeper zk;

	private static EPServiceProvider epService;

	private static List<DynamicSub> dynamicSubs;
	private static List<EPStatement> epStatements;
	private static List<ActiveMQhandler> activeMQhandlers;
	private static byte[] currentData;
	private static byte[] newData;

	public static void main(String[] args) {
		logger.info("It is beginning, is it not?");

		// supply a path to the config file as a first argument
		loadConfig(args[0]);
		logger.info("Configuration parsed and loaded.");

		// load esper configuration
		getConfig();

		// init esper
		initEsper();

		// initiliase dynamic subscribers, and start stream processing
		streamProcessing();
	}

	/**
	 * Depending on 'local_file' flag, receives a configuration for Esper Engine either from a local file,
	 * or connects to a ZooKeeper server and regularly pulls for the configuration (usually injected by
	 * the deployment component).
	 */
	private static void getConfig() {
		if (local_file) {
			try { // read from a provided local file
				logger.info(String.format("Reading local file (%s) to initialise Esper engine.", local_epl_path));
				gson = new Gson();
				JsonReader reader = new JsonReader(new FileReader(local_epl_path));
				esperDefJson = gson.fromJson(reader, EsperDefJson.class);
				logger.debug("Configuration file parsed correctly.");
				msgProc = new MessageProcessor();
			} catch (IOException e) {
				logger.error("Problem reading the file: " + e.getMessage());
				logger.error("Problem reading the file: " + e.getMessage());
				System.exit(1);
			}
		} else { // connect to ZooKeeper and pull configuration
			try {
				logger.info("Connecting to ZooKeeper to receive configuration.");
				zooHandler = new ZooHandler(zoo_ip, zoo_port, zoo_app_node);
				zooHandler.createZnode(Long.toString(resource_id));
				zk = zooHandler.getZk();
				currentData = "".getBytes();
				newData = null;

				// byte[] data = null;
				while (newData == null) {
					newData = zooHandler.getCurrentData();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						logger.error("Can't sleep: " + e.getMessage());
					}
				}
				zooWatcher();
				if (!new String(currentData).equals(new String(newData))) {
					currentData = newData;
					String data = new String(currentData);
					gson = new Gson();
					esperDefJson = gson.fromJson(data, EsperDefJson.class);
					logger.info("Configuration parsed: " + data);
					msgProc = new MessageProcessor();
				}
			} catch (Exception e) {
				logger.error("Error establishing connection to zookeeper: " + e.getMessage());
			}
		}
	}

	/**
	 * Initiliase Esper engine with received event definitions.
	 */
	private static void initEsper() {
		// validate the statements
//		StatementValidator.validateEpls(esperDefJson.getStatements());

		Configuration config = new Configuration();
		config.addPlugInSingleRowFunction("persistResult",
				"eu.uk.ncl.di.pet5o.esper.streamProcessing.storage.InfluxHandler", "persistResult");
		epService = EPServiceProviderManager.getDefaultProvider(config);
		logger.debug("Defining the stream properties:");
		// define the stream event
		Map<String, Object> def;
		for (EsperStream stream : esperDefJson.getStreams()) {
			def = new HashMap<>();
			System.out.print(String.format("Stream: %s; fields: ", stream.getEventName()));
			for (EventProperty prop : stream.getEventProperties()) {
				def.put(prop.getKey(), prop.getKeyClass());
				System.out.print(String.format("%s (%s)\t", prop.getKey(), prop.getKeyClass()));
			}
			epService.getEPAdministrator().getConfiguration().addEventType(stream.getEventName(), def);
			System.out.print(" complete definition inputted to Esper.\n");
		}
	}

	private static void streamProcessing() {
		// create a subscriber
		dynamicSubs = new ArrayList<>();
		epStatements = new ArrayList<>();
		for (EsperStatement statement : esperDefJson.getStatements()) {
			DynamicSub dynamicSub = new DynamicSub(statement.getStatement(), statement.getOutput());
			EPStatement epStatement = epService.getEPAdministrator().createEPL(dynamicSub.getStatement());
			epStatement.setSubscriber(dynamicSub);
			dynamicSubs.add(dynamicSub);
			epStatements.add(epStatement);
			logger.info("Statement: " + statement.getStatement() + " added to the engine.");
		}
		// create activemq connectors - streams / input
		activeMQhandlers = new ArrayList<>();
		for (EsperStream stream : esperDefJson.getStreams()) {
			if (stream.getSource().getType().equals("activemq")) {
				ActiveMQhandler activeMQhandler = new ActiveMQhandler(stream.getSource().getIP(),
						stream.getSource().getPort(), stream.getSource().getQueue());
				logger.info("ActiveMQ connector created for stream: " + stream.getEventName());
				activeMQhandlers.add(activeMQhandler);
			}
		}

		// get message on all streams
		int empty_count = 0;
		boolean empty = false;
		while (!empty) {
			for (ActiveMQhandler handler : activeMQhandlers) {
				// is this an input stream?
				boolean skip = false;
				for (EsperStream stream : esperDefJson.getStreams()) {
					if (stream.getEventName().equals(handler.getQueue()) && (stream.getIsInput() == 0)) {
						skip = true;
					}
				}
				if (skip)
					continue;
				/*
				 * Well... handler gets a new message from queue, and we need to
				 * cast it right so we need stream info message processor
				 * prepares hash map and it goes into esper engine.
				 */
				String msg = handler.getMessage();
				if (msg != null) {
					List<Map<String, Object>> events = msgProc.prepEvent(handler.getQueue(), msg,
							esperDefJson.getStreams());
					for (Map<String, Object> event : events) {
						epService.getEPRuntime().sendEvent(event, handler.getQueue());
					}

					// reset the countdown to stop
					empty_count = 0;
				} else {
					empty_count++;
					logger.info("Queue " + handler.getQueue() + " is empty: " + empty_count);
					if (empty_count > 240)
						empty = true;
				}
			}
		}

		// close all connectors (if open)
		for (DynamicSub sub : dynamicSubs) {
			sub.close();
		}
		for (ActiveMQhandler handler : activeMQhandlers) {
			handler.close();
		}
		logger.info("It is done.");
	}

	public static void submitTestEvents() {
		// create event
		/* 01 */
		Map<String, Object> event = new HashMap<String, Object>();
		event.put("ts", System.currentTimeMillis());
		event.put("x", 0.01523);
		event.put("y", 0.85632);
		event.put("z", 1.02534);

		// send event to Esper
		epService.getEPRuntime().sendEvent(event, "AccelEvent");

		/*
		 * 02 and 03 Map<String, Object> event = new HashMap<String, Object>();
		 * event.put("ts", System.currentTimeMillis()); event.put("sqrted",
		 * 0.1156383657625098); epService.getEPRuntime().sendEvent(event,
		 * "SqrtEdEvent"); epService.getEPRuntime().sendEvent(event,
		 * "StepEvent");
		 */
	}

	// load config file
	private static void loadConfig(String configPath) {
		org.apache.commons.configuration.Configuration config;
		try {
			// load values from config file
			config = new PropertiesConfiguration(configPath);
			resource_id = config.getLong("RESOURCE_ID");
			local_file = config.getInt("LOCAL_EPL") != 0;
			local_epl_path = config.getString("LOCAL_EPL_PATH");
			zoo_ip = config.getString("ZOO_IP");
			zoo_port = config.getInt("ZOO_PORT");
			zoo_app_node = config.getString("ZOO_APP_NODE");

		} catch (Exception e) {
			logger.info("Error parsing the config file(" + configPath + "): " + e.getMessage());
			System.exit(1);
		}
	}


	/**
	 * Setup a watcher to detect changes in the configuration.
	 */
	public static void zooWatcher() throws KeeperException, InterruptedException {
		// ************************
		zk.exists(zoo_app_node + "/" + resource_id, new Watcher() {
			@Override
			public void process(WatchedEvent e) {
				System.out.println(
						"************************************************************************************************* this is from watcher 0");
				System.out.println("change type: " + e.getType().equals(EventType.NodeDataChanged));
				System.out.println("path: " + e.getPath());
				if (e.getType().equals(EventType.NodeDataChanged)
						&& e.getPath().equals(zoo_app_node + "/" + resource_id)) {
					try {
						newData = zk.getData(zoo_app_node + "/" + resource_id, new Watcher() {
							@Override
							public void process(WatchedEvent e) {
								System.out.println(
										"************************************************************************************************* this is from watcher 1");
								System.out.println("change type: " + e.getType().equals(EventType.NodeDataChanged));
								System.out.println("path: " + e.getPath());
								try {
									newData = zk.getData(zoo_app_node + "/" + resource_id, null, null);
									currentData = newData;
									String data = new String(currentData);
									gson = new Gson();
									esperDefJson = gson.fromJson(data, EsperDefJson.class);
									logger.info("Configuration parsed: " + data);
									for (DynamicSub sub : dynamicSubs) {
										sub.close();
									}
									dynamicSubs.clear();
									for (ActiveMQhandler handler: activeMQhandlers) {
										handler.close();
									}
									activeMQhandlers.clear();

									msgProc = new MessageProcessor();
									streamProcessing();
								} catch (KeeperException | InterruptedException e1) {
									e1.printStackTrace();
								}
							}

						}, null);

					} catch (KeeperException | InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					if (!new String(currentData).equals(new String(newData))) {
						currentData = newData;
						String data = new String(currentData);
						gson = new Gson();
						esperDefJson = gson.fromJson(data, EsperDefJson.class);
						logger.info("Configuration parsed: " + data);

						for (DynamicSub sub : dynamicSubs) {
							sub.close();
						}
						dynamicSubs.clear();
						for (ActiveMQhandler handler: activeMQhandlers) {
							handler.close();
						}
						activeMQhandlers.clear();
						msgProc = new MessageProcessor();
						streamProcessing();
					}
				}
				try {
					zooWatcher();
				} catch (KeeperException | InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
		});
	}
}