package com.valuepotion.analytics.aggregators;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;

public class LexicalMin implements Aggregator<String, String> {
	private String value;
	
	@Override
	public void add(String s) {
		if (!LineDataTool.isNA(s)) {
			if (LineDataTool.isNA(value)) {
				value = s;
			} else {
				if (value.compareTo(s) > 0) {
					value = s;
				}
			}
		}
	}

	@Override
	public String get() {
		return LineDataTool.isNA(value) ? StringUtils.EMPTY : value;
	}
}
