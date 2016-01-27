package com.valuepotion.analytics;

import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;

import com.valuepotion.analytics.aggregators.Aggregator;

public class RetentionAggregator implements Aggregator<Integer, Integer[]>{
	private int[] retention90 = new int[91];
	
	public RetentionAggregator() {
		Arrays.fill(retention90, 0);
	}
	
	@Override
	public void add(Integer d) {
		int r = d.intValue();
		if (r <= 90) {
			retention90[r]++;
		}
	}

	@Override
	public Integer[] get() {
		return ArrayUtils.toObject(retention90);
	}
}
