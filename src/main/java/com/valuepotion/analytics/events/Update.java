package com.valuepotion.analytics.events;


public class Update extends IntervalEvent {

	public Update(String date) {
		super(date);
	}
	
	public Update(String date, int count) {
		super(date, count);
	}

	@Override
	public String name() {
		return "update";
	}
}
