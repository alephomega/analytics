package com.valuepotion.analytics.aggregators;

import java.util.LinkedList;
import java.util.List;

public abstract class Append<T> implements Aggregator<T[], T[]> {
	protected List<T> values = new LinkedList<T>();

	@Override
	public void add(T[] s) {
		for (T t : s) {
			values.add(t);
		}
	}
}
