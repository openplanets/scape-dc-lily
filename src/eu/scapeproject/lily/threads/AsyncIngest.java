package eu.scapeproject.lily.threads;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.xml.bind.JAXBException;

import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.scapeproject.lily.resource.ConnectionManager;
import eu.scapeproject.model.BitStream;
import eu.scapeproject.model.File;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.LifecycleState.State;
import eu.scapeproject.model.Representation;
import eu.scapeproject.util.ScapeMarshaller;

/**
 * Asynchronous ingest Worker
 *
 * @author ross king
 *
 */

public class AsyncIngest implements Runnable {

	private static final Logger logger = LoggerFactory
			.getLogger(AsyncIngest.class);

	private ConcurrentLinkedQueue<IntellectualEntity> queue;

	private ScapeMarshaller marshaller;
	private Repository repository;
	private static final String NS = "eu.scapeproject.lily";
	private StateMap stateMap;
	private String threadId;

	AsyncIngest(ConcurrentLinkedQueue<IntellectualEntity> concurrentLinkedQueue) {
		this.queue = concurrentLinkedQueue;
		this.stateMap = StateMap.getStateMap();
		this.repository = ConnectionManager.getRepository();
		this.threadId = Thread.currentThread().getName();
		try {
			this.marshaller = ScapeMarshaller.newInstance();
		} catch (JAXBException e) {
			logger.error("Unable to instantiate SCAPE marshaller", e);
		}
		System.out.println("Started AsyncIngest with id: " + threadId);
	}

	@Override
	public void run() {
		if (queue.peek()!=null) {
			IntellectualEntity entity = queue.poll();
			String ieId = entity.getIdentifier().getValue();
			if (!stateMap.setState(ieId, "AsyncIngest started", State.INGESTING)) {
				logger.error(threadId + " unable to set state for entity: " + ieId);
				System.out.println(threadId + " unable to set state for entity: " + ieId);
			}
			try {
				/* get the lily record for the entity */
				Record ieRecord = null;
				System.out.println(threadId + " starting asynchronous ingest with id = " + ieId);
				RecordId recId = repository.getIdGenerator().newRecordId(ieId);
				ieRecord = repository.newRecord(recId);
				ieRecord.setDefaultNamespace(NS);
				ieRecord.setRecordType("IntellectualEntity");
	
				/* check if there's descriptive metadata to persist in lily */
				if (entity.getDescriptive() != null) {
					/* create a datastream holding the DC metadata */
					Blob blob = createBlob(entity.getDescriptive(), "text.html", "descriptive.xml");
					ieRecord.setField("DescriptiveMD", blob);
				}
	
				if (entity.getRepresentations() != null) {
					/* iterate over the entity hierarchy to persist the whole graph */
					ArrayList<Link> repList = new ArrayList<Link>();
					for (Representation r : entity.getRepresentations()) {
						/*
						 * create a lily Object with a hierarchical parent for this
						 * representation
						 */
						Record repRecord = repository.newRecord();
						repRecord.setDefaultNamespace(NS);
						repRecord.setRecordType("Representation");
						if (r.getTitle() != null)
							repRecord.setField("title", r.getTitle());
						if (r.getUsage() != null)
							repRecord.setField("usage", r.getUsage());
	
						/*
						 * add the metadata to the representation object as
						 * datatstreams
						 */
						if (r.getProvenance() != null) {
							Blob blob = createBlob(r.getProvenance(), "text.html",
									"provenance.xml");
							repRecord.setField("provenanceMD", blob);
						}
						if (r.getTechnical() != null) {
							Blob blob = createBlob(r.getTechnical(), "text.html",
									"technical.xml");
							repRecord.setField("techMD", blob);
						}
						if (r.getRights() != null) {
							Blob blob = createBlob(r.getRights(), "text.html",
									"rights.xml");
							repRecord.setField("rightsMD", blob);
						}
						if (r.getSource() != null) {
							Blob blob = createBlob(r.getSource(), "text.html",
									"source.xml");
							repRecord.setField("sourceMD", blob);
						}
	
						if (r.getFiles() != null) {
							/* add the files as children of representations */
							ArrayList<Link> fileList = new ArrayList<Link>();
							for (File f : r.getFiles()) {
								Record fileRecord = repository.newRecord();
								fileRecord.setDefaultNamespace(NS);
								fileRecord.setRecordType("File");
								if (f.getFilename() != null)
									fileRecord
											.setField("filename", f.getFilename());
								if (f.getMimetype() != null)
									fileRecord
											.setField("mimetype", f.getMimetype());
								if (f.getUri() != null)
									fileRecord.setField("uri", f.getUri());
	
								/*
								 * add the bitstreams to the file object as blobs
								 */
								if (f.getTechnical() != null) {
									Blob blob = createBlob(f.getTechnical(),
											"text.html", "technical.xml");
									fileRecord.setField("techMD", blob);
								}
								/* and add the bitstreams as children of the files */
								if (f.getBitStreams() != null) {
									ArrayList<Link> bsList = new ArrayList<Link>();
									for (BitStream bs : f.getBitStreams()) {
										Record bsRecord = repository.newRecord();
										bsRecord.setDefaultNamespace(NS);
										bsRecord.setRecordType("Bitstream");
										if (bs.getTitle() != null)
											bsRecord.setField("title",
													bs.getTitle());
										/*
										 * add the metadata to the bitstream object
										 * as a blob
										 */
										if (bs.getTechnical() != null) {
											Blob blob = createBlob(
													bs.getTechnical(), "text.html",
													"technical.xml");
											bsRecord.setField("techMD", blob);
										}
										bsRecord = repository.create(bsRecord);
										bsList.add(new Link(bsRecord.getId()));
									}
									// Link the bitstream to the file
									fileRecord.setField("Bitstreams", bsList);
								}
								fileRecord = repository.create(fileRecord);
								fileList.add(new Link(fileRecord.getId()));
							}
							// Link the file to the representation
							repRecord.setField("Files", fileList);
						}
						repRecord = repository.create(repRecord);
						repList.add(new Link(repRecord.getId()));
					}
					// Link the representation to the intellectual entity
					ieRecord.setField("Representations", repList);
				}
				ieRecord = repository.create(ieRecord);
				if (!stateMap.removeEntity(ieId)) {
					logger.error(threadId + " unable to remove entity from StateMap: " + ieId);
					System.out.println(threadId + " unable to remove entity from StateMap: " + ieId);
				}
				System.out.println(threadId + " completed asynchronous ingest with id = " + ieId);
			} catch (Exception ex) {
				logger.error("Ingest failed!", ex);
				if (!stateMap.setState(ieId, "AsyncIngest failed", State.INGEST_FAILED)) {
					logger.error(threadId + " unable to set state for entity: " + ieId);
					System.out.println(threadId + " unable to set state for entity: " + ieId);
				}
				System.out.println(threadId + " FAILED asynchronous ingest.");
			}
		}
	}

	/**
	 * @param Object
	 * @param mimeType
	 * @param title
	 * @return Blob
	 * @throws JAXBException
	 * @throws InterruptedException
	 * @throws RepositoryException
	 * @throws IOException
	 */

	private Blob createBlob(Object obj, String mimeType, String title)
			throws JAXBException, RepositoryException, InterruptedException,
			IOException {
		ByteArrayOutputStream sink = new ByteArrayOutputStream();
		marshaller.serialize(obj, sink);
		byte[] descriptionData = sink.toByteArray();
		Blob blob = new Blob(mimeType, (long) descriptionData.length, title);
		OutputStream os = repository.getOutputStream(blob);
		try {
			os.write(descriptionData);
		} catch (IOException e) {
			logger.error("Failed to create blob: " + title, e);
		} finally {
			os.close();
		}
		return blob;
	}

}
