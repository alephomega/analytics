package com.valuepotion.analytics.aggregators;

import java.util.Arrays;

public class OrderedValueCount implements Aggregator<Integer, int[]> {
	private int[] counts;
	
	public OrderedValueCount(Integer size) {
		counts = new int[size];
		Arrays.fill(counts, 0);
	}

	@Override
	public void add(Integer s) {
		int i = s.intValue();
		if (i >= 0 && i < counts.length) {
			counts[i] = counts[i] + 1;
		}
	}

	@Override
	public int[] get() {
		return counts;
	}
}
