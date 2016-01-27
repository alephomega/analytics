package com.valuepotion.analytics;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.valuepotion.analytics.aggregators.Aggregator;
import com.valuepotion.analytics.bases.CustomerEvent;

public class CustomerEventsAggregator<S> implements Aggregator<List<List<CustomerEvent<S>>>, List<List<CustomerEvent<S>>>> {
	
	private List<List<CustomerEvent<S>>> value = new LinkedList<List<CustomerEvent<S>>>();;

	
	@Override
	public void add(List<List<CustomerEvent<S>>> s) {
		if (value.isEmpty()) {
			value.addAll(s);

		} else {
			if (!s.isEmpty()) {
				List<CustomerEvent<S>> group = value.get(value.size() - 1);
				group.addAll(s.get(0));

				for (int i = 1; i < s.size(); i++) {
					value.add(s.get(i));
				}
			}
		}
	}
	
	public void add(String date, S[] summaries) {
		if (summaries.length > 0) {
			if (value.isEmpty()) {
				value.add(new ArrayList<CustomerEvent<S>>());
			} 

			List<CustomerEvent<S>> group = value.get(value.size() - 1);
			group.add(new CustomerEvent<S>(date, summaries[0]));
			
			for (int i = 1; i < summaries.length; i++) {
				group = new ArrayList<CustomerEvent<S>>();
				group.add(new CustomerEvent<S>(date, summaries[i]));
				value.add(group);
			}
		}
	}
	

	@Override
	public List<List<CustomerEvent<S>>> get() {
		return value;
	}
}
