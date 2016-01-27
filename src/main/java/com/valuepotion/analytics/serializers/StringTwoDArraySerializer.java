package com.valuepotion.analytics.serializers;

import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.core.LineDataTool.FieldSeparator;

public class StringTwoDArraySerializer extends TwoDArraySerializer<String> {

	@Override
	public String[][] deserialize(String line, LineDataTool dataTool) {
		if (LineDataTool.isNA(line)) {
			return new String[0][0];
		}
		
		String[] elements = LineDataTool.asFields(FieldSeparator.ELEMENTS, line);
		
		String[][] res = new String[elements.length][];
		for (int i = 0; i < elements.length; i++) {
			res[i] = (LineDataTool.isNA(elements[i]) ? new String[0] : LineDataTool.asFields(FieldSeparator.CHILD_ELEMENTS, elements[i]));
		}
		
		return res;
	}
}
