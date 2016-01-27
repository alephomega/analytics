package com.valuepotion.analytics.serializers;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;

public class IdentitySerializer implements Serializer<String> {

	@Override
	public String deserialize(String line, LineDataTool dataTool) {
		return LineDataTool.isNA(line) ? StringUtils.EMPTY : line;
	}

	@Override
	public String serialize(String t, LineDataTool dataTool) {
		return LineDataTool.isNA(t) ? StringUtils.EMPTY : t;
	}
}
