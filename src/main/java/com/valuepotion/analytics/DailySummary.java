package com.valuepotion.analytics;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.serializers.IntegerSerializer;
import com.valuepotion.analytics.serializers.Serializer;
import com.valuepotion.analytics.serializers.StringArraySerializer;
import com.valuepotion.analytics.serializers.TimeStringArraySerializer;
import com.valuepotion.analytics.serializers.TimeStringSerializer;


public enum DailySummary {
	
	INSTALLATION_COUNT(
			0, 
			new IntegerSerializer()),
	
	SESSION_COUNT(
			1, 
			new IntegerSerializer()),
	
	SESSION_DURATION(
			2, 
			new IntegerSerializer()),
	
	FIRST_SESSION_TIME(
			3, 
			new TimeStringSerializer()),
	
	INSTALLATION_IDS(
			4, 
			new StringArraySerializer()),

	INSTALLATION_TIMES(
			5, 
			new TimeStringArraySerializer()),
	
	SESSIONS(
			6, 
			new DailySessionSummariesSerializer()),
	
	PURCHASES(
			7, 
			new DailyPurchaseSummariesSerializer()),
	
	UPDATES(
			8, 
			new DailyUpdateSummariesSerializer()),

	OS_VERSIONS(
			9, 
			new StringArraySerializer()),
	
	APP_VERSIONS(
			10, 
			new StringArraySerializer()),
	
	DEVICE_MODELS(
			11, 
			new StringArraySerializer()),
	
	COUNTRIES(
			12, 
			new StringArraySerializer()),
	
	BIRTH_YEARS(
			13, 
			new StringArraySerializer()),
	
	GENDERS(
			14, 
			new StringArraySerializer()),
	
	LEVELS(
			15, 
			new StringArraySerializer()),
	
	ATTRIBUTIONS(
			16, 
			new StringArraySerializer());
	
	
	
	private int i;
	private Serializer serializer;
	
	private DailySummary(int i, Serializer serializer) {
		this.i = i;
		this.serializer = serializer;
	}
	
	public int index() {
		return i;
	}
	
	public static int size() {
		return DailySummary.values().length;
	}
	
	public static String[] init() {
		String[] dailySummary = new String[size()];
		Arrays.fill(dailySummary, StringUtils.EMPTY);
		
		return dailySummary;
	}
	
	public static String[] initFields(String s, int length) {
		String[] fields = new String[length];
		Arrays.fill(fields, s);
		
		return fields;
	}
	
	public static String[] asFields(String line) {
		return LineDataTool.asFields(Type.DAILY_SUMMARY.getSymbol(), line);
	}
	
	public static String asLine(String[] dailySummary) {
		return LineDataTool.asLine(Type.DAILY_SUMMARY.getSymbol(), dailySummary);
	}
	
	
	public Object get(String[] dailySummary, LineDataTool dataTool) {
		return serializer.deserialize(dailySummary[i], dataTool);
	}
	
	public void set(String[] dailySummary, Object value, LineDataTool dataTool) {
		dailySummary[i] = serializer.serialize(value, dataTool);
	}
	
	public void add(String[] dailySummary, Object value, LineDataTool dataTool) {
		throw new UnsupportedOperationException();
	}
	
	public String getString(String[] dailySummary, LineDataTool dataTool) {
		return dailySummary[i];
	}
}
