package com.valuepotion.analytics.serializers;

import com.valuepotion.analytics.core.LineDataTool;

public class DoubleSerializer implements Serializer<Double> {

	@Override
	public Double deserialize(String line, LineDataTool dataTool) {
		return LineDataTool.isNA(line) ? Double.valueOf(0) : Double.valueOf(line);
	}

	@Override
	public String serialize(Double t, LineDataTool dataTool) {
		if (t == null) {
			return Double.toString(0d);
		}
		
		return t.toString();
	}
}
