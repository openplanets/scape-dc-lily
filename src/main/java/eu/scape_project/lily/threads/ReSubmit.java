package eu.scape_project.lily.threads;

import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadPoolExecutor;

import eu.scape_project.lily.resource.ConnectionManager;
import eu.scapeproject.model.IntellectualEntity;

public class ReSubmit implements Runnable {

	private ConcurrentLinkedQueue<IntellectualEntity> queue;
	private ThreadPoolExecutor executor;
	private int maxthreads;
	private boolean running;

	ReSubmit(ConcurrentLinkedQueue<IntellectualEntity> concurrentLinkedQueue) {
		this.queue = concurrentLinkedQueue;
		this.executor = IngestQueue.getExecutor();
		this.running = true;
		Properties props = ConnectionManager.getProperties();
		if (props.getProperty("maxthreads")!=null) {		
			this.maxthreads = new Integer(props.getProperty("maxthreads")).intValue();
		} else {
			this.maxthreads = 10;
		}
	}

	@Override
	public void run() {
		while (running) {
			if (!queue.isEmpty()) {
				if (executor.getActiveCount() < maxthreads) {
					IngestQueue.resubmit();
				} else {
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}				
				}
			} else {
				this.running = false;
			}
		}
		
	}

}
