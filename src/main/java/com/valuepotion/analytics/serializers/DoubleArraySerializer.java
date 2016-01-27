package com.valuepotion.analytics.serializers;

import com.valuepotion.analytics.core.LineDataTool;

public class DoubleArraySerializer extends ArraySerializer<Double> {

	@Override
	public Double[] deserialize(String line, LineDataTool dataTool) {
		if (LineDataTool.isNA(line)) {
			return new Double[0];
		}
		
		String[] fields = LineDataTool.asFields(separator, line);

		Double[] res = new Double[fields.length];
		for (int i = 0; i < fields.length; i++) {
			res[i] = LineDataTool.isNA(fields[i]) ? Double.valueOf(0d) : Double.valueOf(fields[i]);
		}
		
		return res;
	}
}
