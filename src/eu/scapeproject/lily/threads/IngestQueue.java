package eu.scapeproject.lily.threads;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.scapeproject.lily.resource.ConnectionManager;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.LifecycleState.State;

/**
 * Singleton class that provides a thread pool and manages
 * the ingest queue
 * 
 * @author ross king
 * 
 */

public class IngestQueue {
	private static final Logger logger = LoggerFactory
			.getLogger(IngestQueue.class);
	private static final IngestQueue INSTANCE = new IngestQueue();
	private int nthreads;
	private int maxthreads;
	private long lifetime;
	// this queue is a listed of Intellectual Entities submitted for ingest
	private static ConcurrentLinkedQueue<IntellectualEntity> queue;
	private StateMap stateMap;
	private static ThreadPoolExecutor executor;
	// this queue is the thread pool queue used by the Executor Service
	private BlockingQueue<Runnable> threadQueue;
	private RejectedExecutionHandlerImpl handler;
	private static Thread resubmitThread;

	private IngestQueue() {
		// the thread pool is configured in properties.config
		// try to default to sensible values if the properties are undefined
		Properties props = ConnectionManager.getProperties();
		if (props.getProperty("numthreads")!=null) {
			this.nthreads = new Integer(props.getProperty("numthreads")).intValue();
		} else {
			this.nthreads = 5;
		}
		if (props.getProperty("maxthreads")!=null) {		
			this.maxthreads = new Integer(props.getProperty("maxthreads")).intValue();
		} else {
			this.maxthreads = 10;
		}
		if (props.getProperty("threadlifetime")!=null) {		
			this.lifetime = new Long(props.getProperty("threadlifetime")).longValue();
		} else {
			this.lifetime = 5000;
		}
		if (maxthreads < nthreads) maxthreads = nthreads*2;
		IngestQueue.queue = new ConcurrentLinkedQueue<IntellectualEntity>();
		this.threadQueue = new ArrayBlockingQueue<Runnable>(maxthreads);
		this.handler = new RejectedExecutionHandlerImpl();
		IngestQueue.executor = new ThreadPoolExecutor(nthreads, maxthreads, lifetime,
				TimeUnit.MILLISECONDS, threadQueue, handler);
		IngestQueue.executor.allowCoreThreadTimeOut(true);
		this.stateMap = StateMap.getStateMap();
	}

	public void addToQueue(String id, IntellectualEntity ie) {
		queue.add(ie);
		stateMap.addEntity(id);
		stateMap.setState(id, "Added to queue", State.NEW);
		executor.execute(new AsyncIngest(queue));
		logger.info("Added an entity to the queue with id = " + id);
		logger.info("There are now " + queue.size()
				+ " items in the queue.");
	}

	public static IngestQueue getIngestQueue() {
		return INSTANCE;
	}

	public boolean shutdownQueue() {
		executor.shutdown();
		while (!executor.isTerminated()) {
		}
		return true;
	}
	
	public static ThreadPoolExecutor getExecutor() {
		return executor;
	}
	
	protected static void resubmit() {
		executor.execute(new AsyncIngest(queue));
	}
	
	protected static void startReSubmitThread() {
		if (resubmitThread==null) {
			Runnable runnable = new ReSubmit(queue);
			resubmitThread = new Thread(runnable);
			resubmitThread.start();
			logger.info("ReSubmit thread was null - now started.");
		} else if (!resubmitThread.isAlive()) {
			Runnable runnable = new ReSubmit(queue);
			resubmitThread = new Thread(runnable);
			resubmitThread.start();			
			logger.info("ReSubmit thread was dead - now re-started.");
		} else {
			logger.info("ReSubmit thread is already running.");
		}
	}

}
