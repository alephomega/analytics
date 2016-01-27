package com.valuepotion.analytics.events;


public abstract class Event implements Comparable<Event> {
	private String date;

	protected Event(String date) {
		this.date = date;
	}
	
	public String getDate() {
		return date;
	}
	
	@Override
	public int hashCode() {
		return getDate().hashCode();
	}

	public abstract String name();
}
