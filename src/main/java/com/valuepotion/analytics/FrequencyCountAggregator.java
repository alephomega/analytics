package com.valuepotion.analytics;

import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;

import com.valuepotion.analytics.aggregators.Aggregator;

public class FrequencyCountAggregator implements Aggregator<Integer, Integer[]> {
	private int[] counts;

	public FrequencyCountAggregator(Integer duration) {
		counts = new int[duration + 1];
		Arrays.fill(counts, 0);
	}
	
	@Override
	public void add(Integer freq) {
		counts[freq]++;
	}
	
	@Override
	public Integer[] get() {
		return ArrayUtils.toObject(counts);
	}
}
