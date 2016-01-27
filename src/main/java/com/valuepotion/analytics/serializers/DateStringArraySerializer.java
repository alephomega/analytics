package com.valuepotion.analytics.serializers;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;


public class DateStringArraySerializer extends ArraySerializer<String> {

	@Override
	public String serialize(String[] obj, LineDataTool dataTool) {
		if (obj == null) {
			return StringUtils.EMPTY;
		}
		
		String[] arr = dataTool.encodeDates(obj);
		return LineDataTool.asLine(separator, arr);
	}
	
	@Override
	public String[] deserialize(String line, LineDataTool dataTool) {
		if (line == null) {
			return new String[0];
		}
		
		return dataTool.decodeDates(LineDataTool.asFields(separator, line));
	}
}
