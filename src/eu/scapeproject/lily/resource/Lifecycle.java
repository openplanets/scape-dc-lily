package eu.scapeproject.lily.resource;

import java.io.IOException;
import java.io.OutputStream;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.bind.JAXBException;

import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;

import eu.scapeproject.lily.threads.StateMap;
import eu.scapeproject.model.LifecycleState;
import eu.scapeproject.model.LifecycleState.State;
import eu.scapeproject.util.ScapeMarshaller;

/**
 * Implementation of the Connector API endpoint "/lifecycle"
 *
 * @author ross king
 *
 */

@Path("/lifecycle")
public class Lifecycle {
	
	private StateMap stateMap;
	private ScapeMarshaller marshaller;
	private Repository repository;
	
	public Lifecycle() {
		this.stateMap = StateMap.getStateMap();
		this.repository = ConnectionManager.getRepository();
		try {
			this.marshaller = ScapeMarshaller.newInstance();
		} catch (JAXBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@GET
	@Path("/{id}")
	@Produces(MediaType.TEXT_XML)
	public StreamingOutput getLifeCycle(@PathParam("id") final String entityId)
			throws Exception {

		return new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException,
					WebApplicationException {
				LifecycleState state = stateMap.getState(entityId);
				// If the entity is in the ingest queue, take the status from the StateMap
				if (state!=null) {
					try {
						marshaller.serialize(state, output);
					} catch (JAXBException e) {
						throw new IOException(e);
					}
				// If the entity has been ingested already, it can be found in the repository
				} else {
					RecordId id = repository.getIdGenerator().newRecordId(entityId);
					Record record = null;
					try {
						record = repository.read(id);
					} catch (RepositoryException | InterruptedException e) {
						// do nothing
					}
					if (record!=null) {
						try {
							marshaller.serialize(new LifecycleState("Entity exists in repository", State.INGESTED), output);
						} catch (JAXBException e) {
							throw new IOException(e);
						}
					} else {
						byte[] out = new String("Entity not found with id = " + entityId).getBytes();
						output.write(out);						
					}
				}
			}
		};
	
	}


}
