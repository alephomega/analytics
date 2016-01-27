package com.valuepotion.analytics.events;

public class Install extends PointEvent {
	private String channel;

	public Install(String time, String date, String channel) {
		super(time, date);
		this.channel = channel;
	}
	
	public String getChannel() {
		return channel;
	}

	@Override
	public String name() {
		return "install";
	}
}
