package com.valuepotion.analytics.serializers;

import com.valuepotion.analytics.core.LineDataTool;

public class IntegerSerializer implements Serializer<Integer> {

	@Override
	public Integer deserialize(String line, LineDataTool dataTool) {
		return LineDataTool.isNA(line) ? Integer.valueOf(0) : Integer.valueOf(line);
	}

	@Override
	public String serialize(Integer t, LineDataTool dataTool) {
		if (t == null) {
			return String.valueOf(0);
		}
		
		return t.toString();
	}
}
