package com.valuepotion.analytics.legacy;


public class Attribution implements Comparable<Attribution> {
	private String id;
	private String time;
	
	Attribution(String id, String time) {
		this.id = id;
		this.time = time;
	}
	
	String getId() {
		return id;
	}
	
	String getTime() {
		return time;
	}

	void setId(String id) {
		this.id = id;
	}

	void setTime(String time) {
		this.time = time;
	}

	@Override
	public int compareTo(Attribution o) {
		return getTime().compareTo(o.getTime());
	}
}
