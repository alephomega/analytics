package com.valuepotion.analytics.aggregators;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;


public class StringArrayReplacement extends Replacement<String[]> {
	
	@Override
	public String[] get() {
		String[] value = super.get();
		if (value == null) {
			return new String[0];
		}
		
		for (int i = 0; i < value.length; i++) {
			if (LineDataTool.isNA(value[i])) {
				value[i] = StringUtils.EMPTY;
			}
		}
		
		return value;
	}
}
