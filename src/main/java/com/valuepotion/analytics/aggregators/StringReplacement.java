package com.valuepotion.analytics.aggregators;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;

public class StringReplacement extends Replacement<String> {
	
	@Override
	public String get() {
		String value = super.get();
		return LineDataTool.isNA(value) ? StringUtils.EMPTY : value;
	}
}
