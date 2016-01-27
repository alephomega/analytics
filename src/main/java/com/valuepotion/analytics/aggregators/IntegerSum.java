package com.valuepotion.analytics.aggregators;

public class IntegerSum implements Aggregator<Integer, Integer> {
	private int total;
	
	@Override
	public void add(Integer s) {
		total += s.intValue();
	}

	@Override
	public Integer get() {
		return Integer.valueOf(total);
	}
}
