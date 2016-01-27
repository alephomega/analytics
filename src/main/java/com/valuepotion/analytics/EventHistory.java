package com.valuepotion.analytics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.valuepotion.analytics.bases.CustomerEvent;
import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.serializers.Serializer;

public class EventHistory<S> {
	
	private Serializer<List<List<CustomerEvent<S>>>> serializer;
	private List<List<CustomerEvent<S>>> events;
	private LineDataTool dataTool;

	public EventHistory(String history, Serializer<List<List<CustomerEvent<S>>>> serializer, LineDataTool dataTool) {
		this.dataTool = dataTool;
		this.serializer = serializer;
		
		this.events = serializer.deserialize(history, dataTool);
	}
	
	public void add(String line) {
		if (line == null) {
			return;
		}
		
		add(new EventHistory<S>(line, serializer, dataTool));
	}

	void add(EventHistory<S> history) {
		if (history == null) {
			return;
		}
		
		if (events.size() == 0) {
			events = history.events;
		} else {
			if (history.events.size() > 0) {
				List<CustomerEvent<S>> group = events.get(events.size() - 1);
				group.addAll(history.events.get(0));

				for (int i = 1; i < history.events.size(); i++) {
					events.add(history.events.get(i));
				}
			}
		}
	}
	
	void add(S[] summaries) {
		if (summaries.length > 0) {
			if (events.size() == 0) {
				events.add(new ArrayList<CustomerEvent<S>>());
			} 

			List<CustomerEvent<S>> group = events.get(events.size() - 1);
			group.add(new CustomerEvent<S>(dataTool.baseDate(), summaries[0]));
			
			for (int i = 1; i < summaries.length; i++) {
				group = new ArrayList<CustomerEvent<S>>();
				group.add(new CustomerEvent<S>(dataTool.baseDate(), summaries[i]));
				events.add(group);
			}
		}
	}
	
	
	public String asLine() {
		return serializer.serialize(events, dataTool);
	}
	
	public int groupSize() {
		return events.size();
	}
	
	public Iterator<CustomerEvent<S>> iterator(int group) {
		return events.get(group).iterator();
	}
	
	public boolean isEmpty() {
		return events.isEmpty();
	}

	public int recency() {
		CustomerEvent<S> lastEvent = lastEvent();
		if (lastEvent == null) {
			return -1;
		}
		
		return dataTool.diffDays(lastEvent.getDate());
	}
	
	public CustomerEvent<S> firstEvent() {
		if (isEmpty()) {
			return null;
		}
		
		for (int i = 0; i < events.size(); i++) {
			List<CustomerEvent<S>> list = events.get(i);
			if (!list.isEmpty()) {
				return list.get(0);
			}
		}
		
		return null;
	}

	public CustomerEvent<S> lastEvent() {
		if (isEmpty()) {
			return null;
		}
		
		for (int i = events.size() - 1; i > -1; i--) {
			List<CustomerEvent<S>> list = events.get(i);
			if (!list.isEmpty()) {
				return list.get(list.size() - 1);
			}
		}
		
		return null;
	}
	
	public List<CustomerEvent<S>> events(String from) {
		List<CustomerEvent<S>> res = new LinkedList<CustomerEvent<S>>();
		
		for (int i = events.size() - 1; i > -1; i--) {
			
			List<CustomerEvent<S>> list = events.get(i);
			for (int j = list.size() - 1; j > -1; j--) {
				CustomerEvent<S> event = list.get(j);
				if (dataTool.diffDays(event.getDate(), from) >= 0) {
					res.add(event);
				} else {
					break;
				}
			}
		}
		
		return res;
	}
}
