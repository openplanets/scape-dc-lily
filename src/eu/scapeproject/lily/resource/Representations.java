package eu.scapeproject.lily.resource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.bind.JAXBException;

import org.lilyproject.repository.api.Blob;
import org.lilyproject.repository.api.FieldNotFoundException;
import org.lilyproject.repository.api.Link;
import org.lilyproject.repository.api.QName;
import org.lilyproject.repository.api.Record;
import org.lilyproject.repository.api.RecordId;
import org.lilyproject.repository.api.Repository;
import org.lilyproject.repository.api.RepositoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.scapeproject.model.BitStream;
import eu.scapeproject.model.File;
import eu.scapeproject.model.Identifier;
import eu.scapeproject.model.Representation;
import eu.scapeproject.util.ScapeMarshaller;

/**
 * Implementation of the Connector API endpoint "/representation"
 *
 * @author ross king
 *
 */

@Path("/representation")
public class Representations {

	private static final Logger logger = LoggerFactory
			.getLogger(Representations.class);

	private ScapeMarshaller marshaller;
    private Repository repository;
    private static final String NS = "eu.scapeproject.lily";

	public Representations() {
		this.repository = ConnectionManager.getRepository();
		try {
			this.marshaller = ScapeMarshaller.newInstance();
	 	} catch (JAXBException e) {
			logger.error("Unable to instantiate SCAPE marshaller", e);
		}
	}

	@GET
	@Path("/{id}")
	@Produces(MediaType.TEXT_XML)
	public StreamingOutput getRepresentation(@PathParam("id") final String repId)
			throws Exception {
		return getRepresentation(repId, "");
	}

	@GET
	@Path("/{id}/{version-id}")
	@Produces(MediaType.TEXT_XML)
	public StreamingOutput getRepresentation(@PathParam("id") final String repId,
			@PathParam("version-id") final String versionId) throws Exception {
		final Representation.Builder representation = new Representation.Builder();
		/* fetch the parent object from lily */
		RecordId id = repository.getIdGenerator().newRecordId(repId);
		Record repRecord = null;
		// get record version, depending on the sent parameters
		if (versionId.equals("")) {
			repRecord = repository.read(id);
		} else {
			repRecord = repository.read(id, new Long(versionId));
		}
		if (repRecord==null) {
			throw new IOException("No record corresponding to entityId: " + repId);
		}
		repRecord.setDefaultNamespace(NS);
		representation.identifier(new Identifier(repRecord.getId().toString()));
		
		InputStream is = null;
		//get the provenance metadata
		try {
		    is = repository.getInputStream(repRecord, new QName(NS, "provenanceMD"));
		    if (is!=null) representation.provenance(marshaller.deserialize(is));
		} catch (FieldNotFoundException ex){
			if (is != null) is.close();
		} finally {
		    if (is != null) is.close();
		}

		//get the rights metadata
		is = null;
		try {
		    is = repository.getInputStream(repRecord, new QName(NS, "rightsMD"));
		    if (is!=null) representation.rights(marshaller.deserialize(is));
		} catch (FieldNotFoundException ex){
			if (is != null) is.close();
		} finally {
		    if (is != null) is.close();
		}

		//get the technical metadata
		is = null;
		try {
		    is = repository.getInputStream(repRecord, new QName(NS, "techMD"));
		    if (is!=null) representation.technical(marshaller.deserialize(is));
		} catch (FieldNotFoundException ex){
			if (is != null) is.close();
		} finally {
		    if (is != null) is.close();
		}
		
		//get the source metadata
		is = null;
		try {
		    is = repository.getInputStream(repRecord, new QName(NS, "sourceMD"));
		    if (is!=null) representation.source(marshaller.deserialize(is));
		} catch (FieldNotFoundException ex){
			if (is != null) is.close();
		} finally {
		    if (is != null) is.close();
		}

		List<File> files = new ArrayList<File>();
		List<Link> fileRecords = repRecord.getField("Files");
		Iterator<Link> it = fileRecords.iterator();
		while (it.hasNext()) {
			Link fileLink = it.next();
			RecordId recId = fileLink.resolve(repRecord.getId(), repository.getIdGenerator());
			Record fileRecord = repository.read(recId);
			files.add(fetchFile(fileRecord));
		}
		representation.files(files);

		return new StreamingOutput() {
			@Override
			public void write(OutputStream output) throws IOException,
					WebApplicationException {
				try {
					marshaller.serialize(representation.build(), output);
				} catch (JAXBException e) {
					throw new IOException(e);
				}
			}
		};
	}

	/**
	 * @param path
	 * @param fileId
	 * @return
	 */
	private File fetchFile(Record fileRecord) throws Exception {
		File.Builder f = new File.Builder();
		fileRecord.setDefaultNamespace(NS);
		f.identifier(new Identifier(fileRecord.getId().toString()));
		try {
			f.filename((String)(fileRecord.getField("filename")));
		} catch (FieldNotFoundException ex){
			// do nothing, this is not really an error!
		}
		try {
			f.mimetype((String)(fileRecord.getField("mimetype")));
		} catch (FieldNotFoundException ex){
			// do nothing, this is not really an error!
		}
		try {
			f.uri((URI)(fileRecord.getField("uri")));
		} catch (FieldNotFoundException ex){
			// do nothing, this is not really an error!
		}

		//get the technical metadata
		InputStream is = null;
		try {
		    is = repository.getInputStream(fileRecord, new QName(NS, "techMD"));
		    if (is!=null) f.technical(marshaller.deserialize(is));
		} catch (FieldNotFoundException ex){
			if (is != null) is.close();
		} finally {
		    if (is != null) is.close();
		}

		/* fetch the children of the file */
		List<BitStream> bitstreams = new ArrayList<BitStream>();
		List<Link> bsRecords = fileRecord.getField("Bitstreams");
		Iterator<Link> it = bsRecords.iterator();
		while (it.hasNext()) {
			Link bsLink = it.next();
			RecordId recId = bsLink.resolve(fileRecord.getId(), repository.getIdGenerator());
			Record bsRecord = repository.read(recId);
			bitstreams.add(fetchBitStream(bsRecord));
		}

		f.bitStreams(bitstreams);
		return f.build();
	}

	/**
	 * @param path
	 * @return
	 */
	private BitStream fetchBitStream(Record bsRecord) throws Exception {
		BitStream.Builder bs = new BitStream.Builder();
		bs.identifier(new Identifier(bsRecord.getId().toString()));
		try {
			bs.title((String)(bsRecord.getField("title")));
		} catch (FieldNotFoundException ex){
			// do nothing, this is not really an error!
		}
		
		//get the technical metadata
		InputStream is = null;
		try {
		    is = repository.getInputStream(bsRecord, new QName(NS, "techMD"));
		    if (is!=null) bs.technical(marshaller.deserialize(is));
		} catch (FieldNotFoundException ex){
			if (is != null) is.close();
		} finally {
		    if (is != null) is.close();
		}

		return bs.build();
	}

	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public Response ingestRepresentation(InputStream src) throws Exception {
		Representation r = marshaller.deserialize(
				Representation.class, src);
		/*
		 * create a lily Object with a hierarchical parent for this representation
		 */
		Record repRecord = null;
		String repId = r.getIdentifier().getValue();
		if (repId.equals("")) {
			repRecord = repository.newRecord();
		} else {
			RecordId id = repository.getIdGenerator().newRecordId(repId);
			repRecord = repository.newRecord(id);
		}
		repRecord.setDefaultNamespace(NS);
		repRecord.setRecordType("Representation");
		if (r.getTitle()!=null) repRecord.setField("title", r.getTitle());
		if (r.getUsage()!=null) repRecord.setField("usage", r.getUsage());

		/* add the metadata to the representation object as datatstreams */
		if (r.getProvenance() != null) {
			Blob blob = createBlob(r.getProvenance(),"text.html", "provenance.xml");
			repRecord.setField("provenanceMD", blob);	
		}
		if (r.getTechnical() != null) {
			Blob blob = createBlob(r.getTechnical(),"text.html", "technical.xml");
			repRecord.setField("techMD", blob);
		}
		if (r.getRights() != null) {
			Blob blob = createBlob(r.getRights(),"text.html", "rights.xml");
			repRecord.setField("rightsMD", blob);
		}
		if (r.getSource() != null) {
			Blob blob = createBlob(r.getSource(),"text.html", "source.xml");
			repRecord.setField("sourceMD", blob);
		}

		if (r.getFiles() != null) {
			/* add the files as children of representations */
			ArrayList<Link> fileList = new ArrayList<Link>();
			for (File f : r.getFiles()) {
				Record fileRecord = null;
				String fileId = f.getIdentifier().getValue();
				if (fileId.equals("")) {
					fileRecord = repository.newRecord();
				} else {
					RecordId id = repository.getIdGenerator().newRecordId(fileId);
					fileRecord = repository.newRecord(id);
				}
				fileRecord.setDefaultNamespace(NS);
				fileRecord.setRecordType("File");
				if (f.getFilename()!=null) fileRecord.setField("filename", f.getFilename());
				if (f.getMimetype()!=null) fileRecord.setField("mimetype", f.getMimetype());
				if (f.getUri()!=null) fileRecord.setField("uri", f.getUri());

				/*
				 * add the bitstreams to the file object as blobs
				 */
				if (f.getTechnical() != null) {
					Blob blob = createBlob(f.getTechnical(),"text.html", "technical.xml");
					fileRecord.setField("techMD", blob);
				}
				/* and add the bitstreams as children of the files */
				if (f.getBitStreams() != null) {
					ArrayList<Link> bsList = new ArrayList<Link>();
					for (BitStream bs : f.getBitStreams()) {
						Record bsRecord = null;
						String bsId = bs.getIdentifier().getValue();
						if (bsId.equals("")) {
							bsRecord = repository.newRecord();
						} else {
							RecordId id = repository.getIdGenerator().newRecordId(bsId);
							bsRecord = repository.newRecord(id);
						}
						bsRecord.setDefaultNamespace(NS);
						bsRecord.setRecordType("Bitstream");
						if (bs.getTitle()!=null) bsRecord.setField("title", bs.getTitle());
						/*
						 * add the metadata to the bitstream object as a blob
						 */
						if (bs.getTechnical() != null) {
							Blob blob = createBlob(bs.getTechnical(),"text.html", "technical.xml");
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

		return Response.status(201).entity(r.getIdentifier().getValue())
				.build();
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
	
	private Blob createBlob(Object obj, String mimeType, String title) throws JAXBException, RepositoryException, InterruptedException, IOException {
		ByteArrayOutputStream sink = new ByteArrayOutputStream();
		marshaller.serialize(obj, sink);
		byte[] descriptionData = sink.toByteArray();
		Blob blob = new Blob(mimeType, (long)descriptionData.length, title);
		OutputStream os = repository.getOutputStream(blob);
		try {
		    os.write(descriptionData);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		    os.close();
		}
		return blob;
	}
}
