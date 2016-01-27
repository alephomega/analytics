package com.valuepotion.analytics.serializers;

import com.valuepotion.analytics.core.LineDataTool;

public class DateStringSerializer implements Serializer<String> {

	@Override
	public String deserialize(String line, LineDataTool dataTool) {
		return dataTool.decodeDate(line);
	}

	@Override
	public String serialize(String t, LineDataTool dataTool) {
		return dataTool.encodeDate(t);
	}
}
