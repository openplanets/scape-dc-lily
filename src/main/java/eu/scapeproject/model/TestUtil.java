// imported from https://github.com/openplanets/scape-platform-datamodel/blob/066afd903874273ec94b53b6b208e47a9485c020/src/test/java/eu/scapeproject/model/TestUtil.java
package eu.scapeproject.model;

import gov.loc.mix.v20.ImageCaptureMetadataType;
import gov.loc.mix.v20.ImageCaptureMetadataType.DigitalCameraCapture;
import gov.loc.mix.v20.Mix;
import gov.loc.mix.v20.StringType;
import info.lc.xmlns.premis_v2.CopyrightInformationComplexType;
import info.lc.xmlns.premis_v2.EventComplexType;
import info.lc.xmlns.premis_v2.LinkingAgentIdentifierComplexType;
import info.lc.xmlns.premis_v2.ObjectFactory;
import info.lc.xmlns.premis_v2.PremisComplexType;
import info.lc.xmlns.premis_v2.RightsComplexType;
import info.lc.xmlns.premis_v2.RightsStatementComplexType;
import info.lc.xmlns.textmd_v3.TextMD;
import info.lc.xmlns.textmd_v3.TextMD.Encoding;
import info.lc.xmlns.textmd_v3.TextMD.Encoding.EncodingPlatform;

import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;

import org.purl.dc.elements._1.ElementContainer;
import org.purl.dc.elements._1.SimpleLiteral;

import edu.harvard.hul.ois.xml.ns.fits.fits_output.FileInfoType;
import edu.harvard.hul.ois.xml.ns.fits.fits_output.Fits;
import eu.scapeproject.util.ScapeMarshaller;

public abstract class TestUtil {

	public static final Random rand = new Random();
	public static final DateFormat dateFormatter = new SimpleDateFormat("d-M-y hh:mm:ss");

	public static IntellectualEntity createTestEntity() throws JAXBException {
		BitStream bs_1 = new BitStream.Builder()
				.identifier(new Identifier("bitstream:1"))
				.title("Sequence 1")
				.technical(TestUtil.createFITSRecord())
				.build();

		File f = new File.Builder()
				.bitStreams(Arrays.asList(bs_1))
				.identifier(new Identifier("file-1"))
				.uri(URI.create("http://example.com/data"))
				.technical(TestUtil.createMIXRecord())
				.build();

		Representation rep = new Representation.Builder(new Identifier("representation-1"))
				.files(Arrays.asList(f))
				.technical(TestUtil.createTextMDRecord())
				.title("Text representation")
				.provenance(TestUtil.createPremisDigiProvRecord())
				.rights(TestUtil.createPremisRightsRecord())
				.source(TestUtil.createDCSourceRecord())
				.build();

		IntellectualEntity e = new IntellectualEntity.Builder()
				.identifier(new Identifier("entity-1"))
				.representations(Arrays.asList(rep))
				.descriptive(TestUtil.createDCRecord())
				.build();
		return e;
	}

	private static Fits createFITSRecord() {
	    Fits f = new Fits();
	    f.setTimestamp(String.valueOf(new Date().getTime()));
	    return f;
	}

    private static Mix createMIXRecord() {
	    Mix mix = new Mix();
	    ImageCaptureMetadataType capture = new ImageCaptureMetadataType();
	    DigitalCameraCapture camera = new DigitalCameraCapture();
	    StringType val = new StringType();
	    val.setValue("Nikon");
	    val.setUse("ALL");
	    camera.setDigitalCameraManufacturer(val);
	    capture.setDigitalCameraCapture(camera);
	    mix.setImageCaptureMetadata(capture);
	    return mix;
	}

    public static TextMD createTextMDRecord() {
		TextMD textMd = new TextMD();
		Encoding enc = new Encoding();
		EncodingPlatform pf = new EncodingPlatform();
		pf.setLinebreak("LF");
		enc.getEncodingPlatform().add(pf);
		textMd.getEncoding().add(enc);
		return textMd;
	}

	public static JAXBElement<PremisComplexType> createPremisDigiProvRecord() {
		ObjectFactory premisFac = new ObjectFactory();
		PremisComplexType premis = premisFac.createPremisComplexType();
		EventComplexType e = premisFac.createEventComplexType();
		e.setEventDetail("inital ingest");
		e.setEventType("INGEST");
		LinkingAgentIdentifierComplexType agentId = premisFac.createLinkingAgentIdentifierComplexType();
		agentId.setRole("CREATOR");
		agentId.setTitle("Testman Testrevicz");
		e.getLinkingAgentIdentifier().add(agentId);
		premis.getEvent().add(e);
		return premisFac.createPremis(premis);
	}

	public static JAXBElement<RightsComplexType> createPremisRightsRecord() {
		ObjectFactory premisFac = new ObjectFactory();
		RightsComplexType rights = premisFac.createRightsComplexType();
		RightsStatementComplexType stat = premisFac.createRightsStatementComplexType();
		CopyrightInformationComplexType cr = premisFac.createCopyrightInformationComplexType();
		cr.setCopyrightJurisdiction("de");
		cr.setCopyrightStatus("no copyright");
		stat.setCopyrightInformation(cr);
		rights.getRightsStatementOrRightsExtensionOrMdSec().add(stat);
		return premisFac.createRights(rights);
	}

	public static ElementContainer createDCSourceRecord() {
		org.purl.dc.elements._1.ObjectFactory dcFac = new org.purl.dc.elements._1.ObjectFactory();
		ElementContainer cnt = dcFac.createElementContainer();
		SimpleLiteral lit_title = new SimpleLiteral();
		lit_title.getContent().add("Source object 1");
		cnt.getAny().add(dcFac.createTitle(lit_title));
		return cnt;
	}

	public static ElementContainer createDCRecord() {
		org.purl.dc.elements._1.ObjectFactory dcFac = new org.purl.dc.elements._1.ObjectFactory();
		ElementContainer cnt = dcFac.createElementContainer();
		SimpleLiteral lit_title = new SimpleLiteral();
		lit_title.getContent().add("Object 1");
		cnt.getAny().add(dcFac.createTitle(lit_title));
		return cnt;
	}

}
