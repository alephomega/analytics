package com.valuepotion.analytics.events;


public class Session extends IntervalEvent {

	public Session(String date) {
		super(date);
	}

	public Session(String date, int count, int duration) {
		super(date, count);
		this.duration = duration;
	}

	private int duration;

	public int getDuration() {
		return duration;
	}

	public void setDuration(int duration) {
		this.duration = duration;
	}

	@Override
	public String name() {
		return "session";
	}
}
