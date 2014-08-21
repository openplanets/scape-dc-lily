package eu.scape_project.lily.resource;

import java.io.InputStream;

import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.JAXBException;

import eu.scape_project.lily.threads.IngestQueue;
import eu.scapeproject.model.Identifier;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.util.ScapeMarshaller;

/**
 * Implementation of the Connector API endpoint "/entity-async"
 *
 * @author frank asseg
 * @author ross king
 *
 */

@Path("/entity-async")
public class EntityAsync {
	private static final Logger logger = LoggerFactory
			.getLogger(EntityAsync.class);
	private IngestQueue ingestQueue;
	private ScapeMarshaller marshaller;
    private Repository repository;
    
	public EntityAsync() {
		this.ingestQueue = IngestQueue.getIngestQueue();
		this.repository = ConnectionManager.getRepository();
		try {
			this.marshaller = ScapeMarshaller.newInstance();
		} catch (JAXBException e) {
			logger.error("Unable to instantiate ScapeMarshaller!", e);
		}
	}

	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public Response ingestEntity(InputStream src) throws Exception {
		IntellectualEntity entity = marshaller.deserialize(
				IntellectualEntity.class, src);
		
		String ieId = entity.getIdentifier().getValue();
		if (ieId==null) ieId="";
		if (ieId.equals("")) {
			// no id supplied; ask repository to create a new id
			RecordId id = repository.getIdGenerator().newRecordId(ieId);
			ieId = id.toString();
			IntellectualEntity.Builder entityBuilder = new IntellectualEntity.Builder(entity);
			entityBuilder.identifier(new Identifier(ieId)).build();
		} 
		
		/* Now add this record to the ingest queue */
		ingestQueue.addToQueue(ieId, entity);
		return Response
				.status(201)
				.type(MediaType.TEXT_PLAIN)
				.entity("<scape:value>"+entity.getIdentifier().getValue()+"</scape:value>")
				.build();
	}

}
