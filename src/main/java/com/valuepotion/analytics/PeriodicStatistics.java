package com.valuepotion.analytics;

import java.lang.reflect.Constructor;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.aggregators.Aggregator;
import com.valuepotion.analytics.aggregators.IntegerSum;
import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.serializers.IntegerSerializer;
import com.valuepotion.analytics.serializers.Serializer;

public enum PeriodicStatistics {
	WAU(
			0,
			new IntegerSerializer(),
			IntegerSum.class
	),
	
	MAU(
			1,
			new IntegerSerializer(),
			IntegerSum.class
	);
	
	
	private int i;
	private Serializer serializer;
	private Class<? extends Aggregator> aggregator;
	private Object[] params;

	private PeriodicStatistics(int index, Serializer serializer, Class<? extends Aggregator> aggregator, Object... params) {
		this.i = index;
		this.serializer = serializer;
		this.aggregator = aggregator;
		this.params = params;
	}
	
	public static Aggregator[] aggregators() throws Exception {
		PeriodicStatistics[] values = values();
		
		Aggregator[] aggregators = new Aggregator[values.length];
		for (int i = 0; i < values.length; i++) {
			if (values[i].params != null) {
				
				Class[] parameterTypes = new Class[values[i].params.length];
				for (int j = 0; j < values[i].params.length; j++) {
					parameterTypes[j] = values[i].params[j].getClass();
				}
				
				Constructor<? extends Aggregator> constructor = values[i].aggregator.getConstructor(parameterTypes);
				aggregators[i] = constructor.newInstance(values[i].params);
			} else {
				aggregators[i] = values[i].aggregator.newInstance();
			}
		}
		
		return aggregators;
	}
	
	
	public void add(Object value, Aggregator[] aggregators) {
		aggregators[i].add(value);
	}
	
	
	public Object get(Aggregator[] aggregators) {
		
		switch(this) {
		
		case WAU:
			
		case MAU:

			return (Integer) aggregators[i].get();
		
		default:
			return StringUtils.EMPTY;
		}
	}
	
	public Object get(String[] statistics, LineDataTool dataTool) {
		return serializer.deserialize(statistics[i], dataTool);
	}
	
	public void set(String[] statistics, Object value, LineDataTool dataTool) {
		statistics[i] = serializer.serialize(value, dataTool);
	}
	
	public String asLine(Aggregator[] aggregators, LineDataTool dataTool) {
		
		switch(this) {
		
		case WAU:
			
		case MAU:
			
			return ((Integer) aggregators[i].get()).toString();
		
		default:
			return StringUtils.EMPTY;
		}
	}
	
	public static String[] asLines(Aggregator[] aggregators, LineDataTool dataTool) {
		PeriodicStatistics[] values = PeriodicStatistics.values();
		
		String[] lines = new String[values.length];
		for (int i = 0; i < values.length; i++) {
			lines[i] = values[i].asLine(aggregators, dataTool);
		}
		
		return lines;
	}
	
	public static int size() {
		return PeriodicStatistics.values().length;
	}
	public static String[] init() {
		String[] statistics = new String[size()];
		Arrays.fill(statistics, StringUtils.EMPTY);
		
		return statistics;
	}
	
	public static String[] asFields(String line) {
		return LineDataTool.asFields(line);
	}
	
	public static String asLine(String[] statistics) {
		return LineDataTool.asLine(statistics);
	}
}
