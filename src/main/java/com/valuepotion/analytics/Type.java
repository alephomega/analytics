package com.valuepotion.analytics;

import java.util.HashMap;
import java.util.Map;


public enum Type {
	
	DAILY_SUMMARY("~"),
	ATTRIBUTES("*"),
	ATTRIBUTIONS("@"),
	ILLEGAL(null);
	
	private final String symbol;
	
	private static final Map<String, Type> symbolToType = new HashMap<String, Type>();
	static {
		symbolToType.put(DAILY_SUMMARY.getSymbol(), DAILY_SUMMARY);
		symbolToType.put(ATTRIBUTES.getSymbol(), ATTRIBUTES);
		symbolToType.put(ATTRIBUTIONS.getSymbol(), ATTRIBUTIONS);
	}

	private Type(String symbol) {
		this.symbol = symbol;
	}
	
	public static Type fromSymbol(String line) {
		if (line == null || line.length() == 0) {
			return ILLEGAL;
		}
		
		Type type = symbolToType.get(line.substring(0, 1));
		return type == null ? ILLEGAL : type;
	}
	
	public String getSymbol() {
		return symbol;
	}
}