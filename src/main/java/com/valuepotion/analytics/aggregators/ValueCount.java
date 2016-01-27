package com.valuepotion.analytics.aggregators;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.valuepotion.analytics.bases.Pair;

public class ValueCount implements Aggregator<String, Pair<String, Integer>[]> {
	
	private Map<String, Integer> count = new HashMap<String, Integer>();

	@Override
	public void add(String s) {
		if (s != null) {
			Integer cv = count.get(s);
			if (cv == null) {
				cv = Integer.valueOf(1);
			} else {
				cv = Integer.valueOf(cv.intValue() + 1);
			}
			
			count.put(s, cv);
		}
	}

	@Override
	public Pair<String, Integer>[] get() {
		Pair<String, Integer>[] pairs = new Pair[count.size()];
		
		Iterator<Entry<String, Integer>> iterator = count.entrySet().iterator();
		
		int i = 0;
		while (iterator.hasNext()) {
			Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iterator.next();
			pairs[i++] = new Pair<String, Integer>(entry.getKey(), entry.getValue());
		}
		
		return pairs;
	}
}
