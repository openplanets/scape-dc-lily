package eu.scape_project.lily.resource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import com.google.common.net.HttpHeaders;

import eu.scape_project.lily.threads.IngestQueue;
import eu.scape_project.lily.threads.StateMap;

/**
 * A REST service that delivers an HTML page at "/monitor"
 * This page gives useful information about the thread pool,
 * ingest queue, and links to ingested entities.
 * 
 * This class DOES NOT SCALE! For small demonstrations only.
 *
 * @author ross king
 *
 */

@Path("/monitor")
public class Monitor {

	@GET
	public Response getMonitor(@Context HttpServletRequest hsr) {

		String contextPath = hsr.getContextPath();
		StateMap stateMap = StateMap.getStateMap();
		ArrayList<String> ingested = stateMap.getIngested();
		ThreadPoolExecutor executor = IngestQueue.getExecutor();
		Set<String> stateKeys = stateMap.getStateKeys();
		String output = "<h1>Lily Ingest Monitor</h1><hr>";

		// State of Thread Pool
		output += "<h2>Executor Service</h2>";
		output += "<table cellspacing=5 frame=border>";
		output += "<tr><td>Current number of threads:</td><td>"
				+ executor.getPoolSize() + "</td></tr>";
		output += "<tr><td>Current active threads:</td><td>"
				+ executor.getActiveCount() + "</td></tr>";
		output += "<tr><td>Tasks scheduled:</td><td>" + executor.getTaskCount()
				+ "</td></tr>";
		output += "<tr><td>Tasks completed:</td><td>"
				+ executor.getCompletedTaskCount() + "</td></tr>";
		output += "</table><hr>";

		output += "<table cellspacing=15><tr><td><h2>Ingest Queue</h2></td><td><h2>Ingested Entities</h2></td></tr>";

		// State of Ingest Queue
		output += "<tr><td valign=top>";
		if (stateKeys.isEmpty()) {
			output += "<h3>No entities queued for ingest.</h3>";
		} else {
			output += "<table cellspacing=7 frame=border><tr><td><b>ID</b></td><td><b>STATUS</b></td></tr>";
			Iterator<String> it = stateKeys.iterator();
			while (it.hasNext()) {
				String id = it.next();
				output += "<tr><td>" + id + "</td><td>"
						+ stateMap.getState(id).getState() + "</td></tr>";
			}
			output += "</table>";
		}
		output += "</td>";

		// List ingested objects
		output += "<td valign=top>";
		if (ingested.isEmpty()) {
			output += "<h3>No entities ingested.</h3>";
		} else {
			output += "<table cellspacing=7 frame=border>";
			Iterator<String> it3 = ingested.iterator();
			while (it3.hasNext()) {
				String id = it3.next();
				// Note: if you change the REST context in the web.xml,
				// you must change it here as well
				output += "<tr><td><a href=" + contextPath + "/entity/"
						+ id + " target=_blank>" + id + "</a></td><td>";
			}
			output += "</table>";
		}
		output += "</td></tr>";

		output += "</table><hr>";
		
		// Setting the REFRESH header causes the page to automatically reload
		// In this case, every second
		return Response.status(200).header(HttpHeaders.REFRESH, "1")
				.entity(output).build();

	}

}