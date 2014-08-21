/**
 *
 */
package eu.scape_project.lily.integration;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.Before;
import org.junit.Test;

import eu.scapeproject.model.BitStream;
import eu.scapeproject.model.File;
import eu.scapeproject.model.Identifier;
import eu.scapeproject.model.IntellectualEntity;
import eu.scapeproject.model.Representation;
import eu.scapeproject.model.TestUtil;
import eu.scapeproject.util.ScapeMarshaller;

/**
 * @author frank asseg
 * @author ross king
 *
 */
public class IntellectualEntitiesIT {
    private static final String SCAPE_URL = "http://localhost:8080/dc-lily";

    private final DefaultHttpClient client = new DefaultHttpClient();

    private ScapeMarshaller marshaller;

    @Before
    public void setup() throws Exception {
        this.marshaller = ScapeMarshaller.newInstance();
    }

    @Test
    public void testIngestAndRetrieveEntity() throws Exception {
        BitStream bs_1 = new BitStream.Builder()
                .identifier(new Identifier("bitstream-1")).title("Sequence 1")
                .technical(TestUtil.createTextMDRecord()).build();

        File f = new File.Builder().bitStreams(Arrays.asList(bs_1))
                .identifier(new Identifier("file-1"))
                .uri(URI.create("http://example.com/data"))
                .technical(TestUtil.createTextMDRecord()).build();

        Representation rep = new Representation.Builder(new Identifier(
                "representation-1")).files(Arrays.asList(f))
                .technical(TestUtil.createTextMDRecord())
                .title("Text representation")
                .provenance(TestUtil.createPremisDigiProvRecord())
                .rights(TestUtil.createPremisRightsRecord())
                .source(TestUtil.createDCSourceRecord()).build();

        IntellectualEntity e = new IntellectualEntity.Builder()
                .identifier(new Identifier("entity-1"))
                .representations(Arrays.asList(rep))
                .descriptive(TestUtil.createDCRecord()).build();

        HttpPost post = new HttpPost(SCAPE_URL + "/entity");

        ByteArrayOutputStream sink = new ByteArrayOutputStream();
        marshaller.serialize(e, sink);
        post.setEntity(new InputStreamEntity(new ByteArrayInputStream(sink.toByteArray()), sink.size()));

        HttpResponse resp = client.execute(post);
        post.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 201);

        HttpGet getEntity = new HttpGet(SCAPE_URL + "/entity/" + e.getIdentifier().getValue());
        resp = client.execute(getEntity);
        System.out.println(IOUtils.toString(resp.getEntity().getContent()));
        getEntity.releaseConnection();
        assertTrue(resp.getStatusLine().getStatusCode() == 200);
    }
}
