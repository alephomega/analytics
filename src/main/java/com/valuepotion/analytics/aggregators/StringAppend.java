package com.valuepotion.analytics.aggregators;

public class StringAppend extends Append<String> {
	
	@Override
	public String[] get() {
		return values.toArray(new String[0]);
	}
}
