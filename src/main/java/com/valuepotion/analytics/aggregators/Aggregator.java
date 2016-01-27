package com.valuepotion.analytics.aggregators;

public interface Aggregator<S, T> {

	void add(S s);
	T get();
}
