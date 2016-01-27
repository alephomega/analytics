package com.valuepotion.analytics.serializers;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.core.LineDataTool.FieldSeparator;

public abstract class TwoDArraySerializer<T> implements Serializer<T[][]> {

	@Override
	public String serialize(T[][] obj, LineDataTool dataTool) {
		if (obj == null) {
			return StringUtils.EMPTY;
		}
		
		String[] elements = new String[obj.length];
		for (int i = 0; i < obj.length; i++) {
			
			if (obj[i] == null) {
				elements[i] = StringUtils.EMPTY;
			} else {
				Object[] children = new Object[obj[i].length];
				for (int j = 0; j < obj[i].length; j++) {
					children[j] = (obj[i][j] == null ? StringUtils.EMPTY : obj[i][j].toString());
				}
				
				elements[i] = LineDataTool.asLine(FieldSeparator.CHILD_ELEMENTS, children);
			}
		}
		
		return LineDataTool.asLine(FieldSeparator.ELEMENTS, elements);
	}
}
