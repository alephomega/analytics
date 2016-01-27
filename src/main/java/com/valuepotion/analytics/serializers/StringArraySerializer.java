package com.valuepotion.analytics.serializers;

import com.valuepotion.analytics.core.LineDataTool;

public class StringArraySerializer extends ArraySerializer<String> {

	@Override
	public String[] deserialize(String line, LineDataTool dataTool) {
		if (LineDataTool.isNA(line)) {
			return new String[0];
		}
		
		return LineDataTool.asFields(separator, line);
	}
}
