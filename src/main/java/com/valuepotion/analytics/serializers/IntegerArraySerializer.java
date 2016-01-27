package com.valuepotion.analytics.serializers;

import com.valuepotion.analytics.core.LineDataTool;

public class IntegerArraySerializer extends ArraySerializer<Integer> {

	@Override
	public Integer[] deserialize(String line, LineDataTool dataTool) {
		if (LineDataTool.isNA(line)) {
			return new Integer[0];
		}
		
		String[] fields = LineDataTool.asFields(separator, line);
		Integer[] res = new Integer[fields.length];
		for (int i = 0; i < fields.length; i++) {
			res[i] = LineDataTool.isNA(fields[i]) ? Integer.valueOf(0) : Integer.valueOf(fields[i]);
		}
		
		return res;
	}
}
