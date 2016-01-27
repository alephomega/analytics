package com.valuepotion.analytics.serializers;

import com.valuepotion.analytics.core.LineDataTool;


public interface Serializer<T> {
	
	public T deserialize(String line, LineDataTool dataTool);
	
	public String serialize(T t, LineDataTool dataTool);
}
