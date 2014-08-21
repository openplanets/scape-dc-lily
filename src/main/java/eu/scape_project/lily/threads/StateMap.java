package eu.scape_project.lily.threads;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import eu.scapeproject.model.LifecycleState;
import eu.scapeproject.model.LifecycleState.State;

/**
 * Singleton HashMap of the state of IntellectualEntities in the Ingest Queue
 * 
 * @author ross king
 *
 */

public class StateMap {
	
	private static final StateMap INSTANCE = new StateMap();
	private HashMap<String,LifecycleState> entityStateMap;
	
	/* This ArrayList is only used for the Monitor endpoint
	   It is the source of a memory leak, as ingested IDs are
	   added forever and never removed */
	private ArrayList<String> ingested;
	
	private StateMap() {
		entityStateMap = new HashMap<String,LifecycleState>();
		ingested = new ArrayList<String>();
	}
	
	public static StateMap getStateMap() {
		return INSTANCE;
	}
	
	public synchronized boolean addEntity(String id) {
		if (entityStateMap.containsKey(id)) {
			return false;
		} else {
			LifecycleState state = new LifecycleState("Entity added to ingest queue", State.NEW);
			entityStateMap.put(id, state);
			return true;
		}
	}
	
	public synchronized boolean removeEntity(String id) {
		if (!entityStateMap.containsKey(id)) {
			return false;
		} else {
			entityStateMap.remove(id);
			ingested.add(id);
			return true;
		}
	}
	
	public synchronized boolean setState(String id, String details, State state) {
		if (!entityStateMap.containsKey(id)) {
			return false;
		} else {
			LifecycleState lstate = new LifecycleState(details, state);
			entityStateMap.remove(id);
			entityStateMap.put(id, lstate);
			return true;
		}
	}
	
	public synchronized LifecycleState getState(String id) {
		if (!entityStateMap.containsKey(id)) {
			return null;
		} else {
			return entityStateMap.get(id);
		}
	}
	
	public Set<String> getStateKeys() {
		return entityStateMap.keySet();
	}
	
	public ArrayList<String> getIngested() {
		return ingested;
	}

}
