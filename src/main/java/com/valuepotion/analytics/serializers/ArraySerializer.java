package com.valuepotion.analytics.serializers;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.core.LineDataTool.FieldSeparator;

public abstract class ArraySerializer<T> implements Serializer<T[]> {
	protected FieldSeparator separator;

	public ArraySerializer() {
		this.separator = FieldSeparator.ELEMENTS;
	}

	ArraySerializer(FieldSeparator separator) {
		this.separator = separator;
	}
	

	@Override
	public String serialize(T[] obj, LineDataTool dataTool) {
		if (obj == null) {
			return StringUtils.EMPTY;
		}
		
		String[] arr = new String[obj.length];
		for (int i = 0; i < obj.length; i++) {
			arr[i] = (obj[i] == null ? StringUtils.EMPTY : obj[i].toString());
		}
		
		return LineDataTool.asLine(separator, arr);
	}
}
