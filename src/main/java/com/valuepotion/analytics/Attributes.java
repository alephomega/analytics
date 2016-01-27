package com.valuepotion.analytics;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.aggregators.Aggregator;
import com.valuepotion.analytics.aggregators.IntegerSum;
import com.valuepotion.analytics.aggregators.LexicalMin;
import com.valuepotion.analytics.aggregators.StringAppend;
import com.valuepotion.analytics.aggregators.StringArrayReplacement;
import com.valuepotion.analytics.bases.CustomerEvent;
import com.valuepotion.analytics.bases.Timeline;
import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.events.Purchase;
import com.valuepotion.analytics.events.Session;
import com.valuepotion.analytics.events.Update;
import com.valuepotion.analytics.serializers.DateStringArraySerializer;
import com.valuepotion.analytics.serializers.DateStringSerializer;
import com.valuepotion.analytics.serializers.IntegerSerializer;
import com.valuepotion.analytics.serializers.Serializer;
import com.valuepotion.analytics.serializers.StringArraySerializer;
import com.valuepotion.analytics.serializers.TimeStringArraySerializer;
import com.valuepotion.analytics.serializers.TimeStringSerializer;

public enum Attributes {
	
	INSTALLATION_COUNT(
			0, 
			new IntegerSerializer(), 
			IntegerSum.class),
	
	SESSION_COUNT(
			1, 
			new IntegerSerializer(), 
			IntegerSum.class),
	
	SESSION_DURATION(
			2, 
			new IntegerSerializer(), 
			IntegerSum.class),
	
	FIRST_SESSION_DATE(
			3, 
			new DateStringSerializer(), 
			LexicalMin.class),
	
	FIRST_SESSION_TIME(
			4, 
			new TimeStringSerializer(), 
			LexicalMin.class),
	
	INSTALLATION_DATES(
			5, 
			new DateStringArraySerializer(), 
			StringAppend.class),
	
	INSTALLATION_TIMES(
			6, 
			new TimeStringArraySerializer(), 
			StringAppend.class),
	
	ATTRIBUTIONS(
			7, 
			new StringArraySerializer(), 
			StringAppend.class),
	
	SESSIONS(
			8, 
			new SessionsSerializer(), 
			SessionsAggregator.class),
	
	PURCHASES(
			9, 
			new PurchasesSerializer(), 
			PurchasesAggregator.class),

	UPDATES(
			10, 
			new UpdatesSerializer(), 
			UpdatesAggregator.class),
	
	OS_VERSIONS(
			11, 
			new StringArraySerializer(), 
			StringArrayReplacement.class),
	
	APP_VERSIONS(
			12, 
			new StringArraySerializer(), 
			StringArrayReplacement.class),
	
	DEVICE_MODELS(
			13, 
			new StringArraySerializer(), 
			StringArrayReplacement.class),
	
	COUNTRIES(
			14, 
			new StringArraySerializer(), 
			StringArrayReplacement.class),
	
	BIRTH_YEARS(
			15, 
			new StringArraySerializer(), 
			StringArrayReplacement.class),
	
	GENDERS(
			16, 
			new StringArraySerializer(), 
			StringArrayReplacement.class),
	
	LEVELS(
			17, 
			new StringArraySerializer(), 
			StringArrayReplacement.class);


	private int i;
	
	private Class<? extends Aggregator> aggregator;
	private Serializer serializer;
	private Object[] params;
	
	
	private Attributes(int i, Serializer serializer, Class<? extends Aggregator> aggregator, Object... params) {
		this.i = i;
		this.serializer = serializer;
		this.aggregator = aggregator;
		this.params = params;
	}
	
	public int getIndex() {
		return i;
	}
	
	public Serializer getSerializer() {
		return serializer;
	}
	
	public static int size() {
		return Attributes.values().length;
	}
	
	public static String[] init() {
		String[] attributes = new String[size()];
		Arrays.fill(attributes, StringUtils.EMPTY);
		
		return attributes;
	}
	
	public static String[] fields(String s, int length) {
		String[] fields = new String[length];
		Arrays.fill(fields, s);
		
		return fields;
	}
	
	public static String[] asFields(String line) {
		return LineDataTool.asFields(Type.ATTRIBUTES.getSymbol(), line);
	}
	
	public static String asLine(String[] attributes) {
		return LineDataTool.asLine(Type.ATTRIBUTES.getSymbol(), attributes);
	}
	
	public static Aggregator[] createAggregators() throws Exception {
		Attributes[] values = values();
		
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
	
	public Object get(Aggregator[] aggregators) {
		return aggregators[i].get();
	}
	
	
	public Object get(String[] attributes, LineDataTool dataTool) {
		return serializer.deserialize(attributes[i], dataTool);
	}
	

	public void set(String[] attributes, Object value, LineDataTool dataTool) {
		attributes[i] = serializer.serialize(value, dataTool);
	}


	public void add(Object value, Aggregator[] aggregators, LineDataTool dataTool) {
		aggregators[i].add(value);
	}
	
	public String getString(String[] attributes, LineDataTool dataTool) {
		return attributes[i];
	}
	
	
	public static Timeline getTimeline(String[] attributes, LineDataTool dataTool) {
		
		Timeline timeline = new Timeline(
				(String) Attributes.FIRST_SESSION_DATE.get(attributes, dataTool), 
				(String) Attributes.FIRST_SESSION_TIME.get(attributes, dataTool), 
				(String[]) Attributes.INSTALLATION_TIMES.get(attributes, dataTool), 
				dataTool);
		
		addSessionEvents(
				timeline, 
				(List<List<CustomerEvent<SessionSummary>>>) Attributes.SESSIONS.get(attributes, dataTool));
		
		addPurchaseEvents(
				timeline, 
				(List<List<CustomerEvent<PurchaseSummary[]>>>) Attributes.PURCHASES.get(attributes, dataTool));
		
		addUpdateEvents(
				timeline, 
				(List<List<CustomerEvent<UpdateSummary>>>) Attributes.UPDATES.get(attributes, dataTool));
		
		return timeline;
		
		
	}

	private static void addSessionEvents(Timeline timeline, List<List<CustomerEvent<SessionSummary>>> groups) {
		for (int i = 0; i < groups.size(); i++) {
			
			List<CustomerEvent<SessionSummary>> events = groups.get(i);
			for (int j = 0; j < events.size(); j++) {
				CustomerEvent<SessionSummary> event = events.get(j);
				
				SessionSummary summary = event.getSummary();
				
				timeline.addEvent(i, new Session(event.getDate(), summary.getCount(), summary.getDuration()));
			}
		}
	}

	private static void addPurchaseEvents(Timeline timeline, List<List<CustomerEvent<PurchaseSummary[]>>> groups) {
		for (int i = 0; i < groups.size(); i++) {
			
			List<CustomerEvent<PurchaseSummary[]>> events = groups.get(i);
			for (int j = 0; j < events.size(); j++) {
				CustomerEvent<PurchaseSummary[]> event = events.get(j);
				
				PurchaseSummary[] summaries = event.getSummary();
				for (PurchaseSummary summary : summaries) {
					timeline.addEvent(i, new Purchase(event.getDate(), summary.getCurrency(), summary.getCount(), summary.getAmount()));
				}
			}
		}
	}
	
	private static void addUpdateEvents(Timeline timeline, List<List<CustomerEvent<UpdateSummary>>> groups) {
		for (int i = 0; i < groups.size(); i++) {
			
			List<CustomerEvent<UpdateSummary>> events = groups.get(i);
			for (int j = 0; j < events.size(); j++) {
				CustomerEvent<UpdateSummary> event = events.get(j);
				
				UpdateSummary summary = event.getSummary();
				timeline.addEvent(i, new Update(event.getDate(), summary.getCount()));
			}
		}
	}

}
