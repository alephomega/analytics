package com.valuepotion.analytics.serializers;

import com.valuepotion.analytics.core.LineDataTool;

public class TimeStringSerializer implements Serializer<String> {

	@Override
	public String deserialize(String line, LineDataTool dataTool) {
		return dataTool.decodeTime(line);
	}

	@Override
	public String serialize(String t, LineDataTool dataTool) {
		return dataTool.encodeTime(t);
	}
}
