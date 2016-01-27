package com.valuepotion.analytics.serializers;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;

public class TimeStringArraySerializer extends ArraySerializer<String> {

	@Override
	public String serialize(String[] obj, LineDataTool dataTool) {
		if (obj == null) {
			return StringUtils.EMPTY;
		}
		
		String[] arr = dataTool.encodeTimes(obj);
		return LineDataTool.asLine(separator, arr);
	}
	
	@Override
	public String[] deserialize(String line, LineDataTool dataTool) {
		if (LineDataTool.isNA(line)) {
			return new String[0];
		}
		
		return dataTool.decodeTimes(LineDataTool.asFields(separator, line));
	}
}
