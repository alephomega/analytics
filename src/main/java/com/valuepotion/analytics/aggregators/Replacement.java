package com.valuepotion.analytics.aggregators;


public abstract class Replacement<T> implements Aggregator<T, T> {
	private T value;

	@Override
	public void add(T s) {
		value = s;
	}

	@Override
	public T get() {
		return value;
	}
}
