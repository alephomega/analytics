package com.valuepotion.analytics;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.serializers.IdentitySerializer;
import com.valuepotion.analytics.serializers.IntegerSerializer;
import com.valuepotion.analytics.serializers.Serializer;
import com.valuepotion.analytics.serializers.StringArraySerializer;

public enum SessionData {

	INSTALLATION_ID(
			0, 
			new IdentitySerializer()),

	INSTALLATION_TIME(
			1, 
			new IdentitySerializer()),

	UPDATE_COUNT(
			2, 
			new IntegerSerializer()),

	PURCHASES(
			3, 
			new StringArraySerializer()),

	SESSION_START_TIME(
			4, 
			new IdentitySerializer()),

	SESSION_DURATION(
			5, 
			new IntegerSerializer()),

	OS_VERSION(
			6, 
			new IdentitySerializer()),

	APP_VERSION(
			7, 
			new IdentitySerializer()),

	DEVICE_MODEL(
			8, 
			new IdentitySerializer()),

	COUNTRY(
			9, 
			new IdentitySerializer()),

	BIRTH_YEAR(
			10, 
			new IdentitySerializer()),

	GENDER(
			11, 
			new IdentitySerializer()),

	LEVELS(
			12, 
			new StringArraySerializer());
	
	
	private int i;
	private Serializer serializer;
	
	private SessionData(int i, Serializer serializer) {
		this.i = i;
		this.serializer = serializer;
	}
	
	public int getIndex() {
		return i;
	}

	
	public static String[] init() {
		String[] sessionRecord = new String[size()];
		Arrays.fill(sessionRecord, StringUtils.EMPTY);
		
		return sessionRecord;
	}
	
	public static int size() {
		return SessionData.values().length;
	}
	
	public static boolean isFirstSession(String[] sessionRecord, LineDataTool dataTool) {
		return !LineDataTool.isNA((String) INSTALLATION_ID.get(sessionRecord, dataTool));
	}
	
	public static String[] asFields(String line) {
		return LineDataTool.asFields(line);
	}
	
	public static String asLine(String[] sessionRecord) {
		return LineDataTool.asLine(sessionRecord);
	}
	
	
	public Object get(String[] sessionRecord, LineDataTool dataTool) {
		return serializer.deserialize(sessionRecord[i], dataTool);
	}
	
	public void set(String[] sessionRecord, Object value, LineDataTool dataTool) {
		sessionRecord[i] = (value == null ? StringUtils.EMPTY : serializer.serialize(value, dataTool));
	}
	
	public String getString(String[] sessionRecord, LineDataTool dataTool) {
		return sessionRecord[i];
	}
}