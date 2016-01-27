package com.valuepotion.analytics;

import java.lang.reflect.Constructor;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.aggregators.Aggregator;
import com.valuepotion.analytics.aggregators.IntegerSum;
import com.valuepotion.analytics.aggregators.ValueCount;
import com.valuepotion.analytics.bases.Pair;
import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.core.LineDataTool.FieldSeparator;


public enum DailyStatistics {
	
	INSTALLATION_COUNT(
			0, 
			IntegerSum.class),
	
	REINSTALLATION_COUNT(
			1, 
			IntegerSum.class),
	
	UPDATE_COUNT(
			2, 
			IntegerSum.class),
	
	DAU(
			3, 
			IntegerSum.class),
	
	SESSIONS(
			4, 
			SessionSummarySum.class),
	
	PURCHASES(
			5, 
			PurchaseSummaryAggregator.class),
	
	OS_VERSIONS(
			6, 
			ValueCount.class),
	
	APP_VERSIONS(
			7, 
			ValueCount.class),
	
	DEVICE_MODELS(
			8, 
			ValueCount.class),
	
	COUNTRIES(
			9, 
			ValueCount.class),
	
	AGES(
			10, 
			ValueCount.class),
	
	GENDERS(
			11, 
			ValueCount.class),
	
	RETENTION90(
			12, 
			RetentionAggregator.class),
	
	REVENUE90(
			13, 
			RevenueAggregator.class, Integer.valueOf(90)),
	
	FREQUENCY_USERS7(
			14, 
			FrequencyCountAggregator.class, Integer.valueOf(7)),
	
	FREQUENCY_USERS30(
			15, 
			FrequencyCountAggregator.class, Integer.valueOf(30)),
	
	FREQUENCY_REVENUE7(
			16, 
			RevenueAggregator.class, Integer.valueOf(7)),
	
	FREQUENCY_REVENUE30(
			17, 
			RevenueAggregator.class, Integer.valueOf(30)),
	
	PAYING_USERS(
			18,
			IntegerSum.class);
	
	
	private int i;
	private Class<? extends Aggregator> aggregator;
	private Object[] params;

	private DailyStatistics(int index, Class<? extends Aggregator> aggregator, Object... params) {
		this.i = index;
		this.aggregator = aggregator;
		this.params = params;
	}

	public static Aggregator[] createAggregators() throws Exception {
		DailyStatistics[] values = values();
		
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
		
		case INSTALLATION_COUNT:
			
		case REINSTALLATION_COUNT:
			
		case UPDATE_COUNT:
			
		case DAU:
			
		case PAYING_USERS:
			return (Integer) aggregators[i].get();
		
		
		case SESSIONS:
			return (SessionSummary) aggregators[i].get();
			
		
		case PURCHASES:
			return (PurchaseSummary[]) aggregators[i].get();

		
		case OS_VERSIONS:
			
		case APP_VERSIONS:
			
		case DEVICE_MODELS:
			
		case COUNTRIES:
			
		case AGES:
			
		case GENDERS:
			return (Pair<String, Integer>[]) aggregators[i].get();
		
			
		default:
			return StringUtils.EMPTY;
		}
	}
	
	public String asLine(Aggregator[] aggregators, LineDataTool dataTool) {
		
		switch(this) {
		
		case INSTALLATION_COUNT:
			
		case REINSTALLATION_COUNT:
			
		case UPDATE_COUNT:
			
		case DAU:
			
		case PAYING_USERS:
			
			return ((Integer) aggregators[i].get()).toString();
		
		case SESSIONS:
			
			SessionSummary summary = (SessionSummary) aggregators[i].get();
			return LineDataTool.asLine(FieldSeparator.ELEMENTS, new Integer[] { summary.getCount(), summary.getDuration() });
			
		case PURCHASES:
			
		{
			PurchaseSummary[] summaries = (PurchaseSummary[]) aggregators[i].get();
			if (summaries.length == 0) {
				return StringUtils.EMPTY;
			}
			
			String[] elements = new String[summaries.length];
			for (int i = 0; i < summaries.length; i++) {
				elements[i] = LineDataTool.asLine(FieldSeparator.CHILD_ELEMENTS, new Object[] { summaries[i].getCurrency(), summaries[i].getCount(), summaries[i].getAmount() });
			}
			
			return LineDataTool.asLine(FieldSeparator.ELEMENTS, elements);
		}
		
		case OS_VERSIONS:
			
		case APP_VERSIONS:
			
		case DEVICE_MODELS:
			
		case COUNTRIES:
			
		case AGES:
			
		case GENDERS:
			
		{
			Pair<String, Integer>[] pairs = (Pair<String, Integer>[]) aggregators[i].get();
			if (pairs.length == 0) {
				return StringUtils.EMPTY;
			}
			
			String[] elements = new String[pairs.length];
			for (int i = 0; i < pairs.length; i++) {
				elements[i] = LineDataTool.asLine(FieldSeparator.CHILD_ELEMENTS, new Object[] { pairs[i].getX(), pairs[i].getY() });
			}
			
			return LineDataTool.asLine(FieldSeparator.ELEMENTS, elements);
		}
		
		case RETENTION90:

		case FREQUENCY_USERS7:
			
		case FREQUENCY_USERS30:
			
			return LineDataTool.asLine(FieldSeparator.ELEMENTS, (Integer[]) (aggregators[i].get()));
			

		case FREQUENCY_REVENUE7:
			
		case FREQUENCY_REVENUE30:
		
		case REVENUE90:
		
		{
			PurchaseSummarySerializer serializer = new PurchaseSummarySerializer();
			PurchaseSummary[][] summaries = (PurchaseSummary[][]) aggregators[i].get();
			String[] groups = new String[summaries.length];
			for (int i = 0; i < summaries.length; i++) {
				groups[i] = serializer.serialize(summaries[i], dataTool);
			}
			
			return LineDataTool.asLine(FieldSeparator.GROUPS_OF_ELEMENTS, groups);
		}
		
		default:
			return StringUtils.EMPTY;
		}
	}
	
	public static String[] asLines(Aggregator[] aggregators, LineDataTool dataTool) {
		DailyStatistics[] values = DailyStatistics.values();
		
		String[] lines = new String[values.length];
		for (int i = 0; i < values.length; i++) {
			lines[i] = values[i].asLine(aggregators, dataTool);
		}
		
		return lines;
	}
}
