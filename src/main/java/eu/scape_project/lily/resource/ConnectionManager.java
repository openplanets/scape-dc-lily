package eu.scape_project.lily.resource;

import java.io.IOException;
import java.util.Properties;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.zookeeper.KeeperException;
import org.lilyproject.client.LilyClient;
import org.lilyproject.client.NoServersException;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.lilyproject.util.zookeeper.ZkConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.scape_project.lily.threads.IngestQueue;

/**
 * Initializes the DC-Lily REST Service - obtains the LilyClient and repository
 * objects - initializes the IngestQueue
 * 
 * This class is instantiated when the web application starts because
 * it is registered as a listener in the web.xml
 * 
 * @author ross king
 * 
 */

public class ConnectionManager implements ServletContextListener {

	private static final Logger logger = LoggerFactory
			.getLogger(ConnectionManager.class);

	private LilyClient lilyClient;
	private String lilyHost;
	private IngestQueue ingestQueue;
	private static Properties properties;
	private static Repository repository;

	public void contextInitialized(ServletContextEvent arg0) {

		System.out.println("ConnectionManager context initialization...");
		properties = new Properties();
		try {
			properties.load(Thread.currentThread().getContextClassLoader()
					.getResourceAsStream("properties.config"));
		} catch (IOException e1) {
			logger.error("Unable to locate properties.config");
		}
		lilyHost = properties.getProperty("lilyhost");
		if (lilyHost==null) {
			// default to localhost
			lilyHost = "localhost:2181";
		} else {
			System.out.println("Got lilyHost from properties file: " + properties.getProperty("lilyhost"));
		}
		try {
			this.lilyClient = new LilyClient(lilyHost, 20000);
			ConnectionManager.repository = lilyClient.getRepository();
		} catch (IOException e) {
			logger.error("IOError!", e);
		} catch (InterruptedException e) {
			logger.error("Interrupted Exception!", e);
		} catch (KeeperException e) {
			logger.error("ZooKeeper Exception!", e);
		} catch (ZkConnectException e) {
			logger.error("ZooKeeper Connect Exception!", e);
		} catch (NoServersException e) {
			logger.error("No Servers Exeception!", e);
		} catch (RepositoryException e) {
			logger.error("Repository Exception!", e);
		}
		logger.info("Lily repository connection established.");

		// Start thread pool
		this.ingestQueue = IngestQueue.getIngestQueue();
		logger.info("Ingest queue initialized and thread pool started.");

	}// end contextInitialized method

	public void contextDestroyed(ServletContextEvent arg0) {

		if (!ingestQueue.shutdownQueue()) {
			logger.error("Unable to shutdown thread pool!");
		}
		try {
			repository.close();
			lilyClient.close();
		} catch (IOException e) {
			logger.error("IO Error while closing repository connections!", e);
		}

	}// end constextDestroyed method

	public static Repository getRepository() {
		return ConnectionManager.repository;
	}
	
	public static Properties getProperties() {
		return ConnectionManager.properties;
	}

}