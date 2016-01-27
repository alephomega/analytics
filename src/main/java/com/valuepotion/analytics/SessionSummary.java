package com.valuepotion.analytics;

import com.valuepotion.analytics.bases.Summary;

public class SessionSummary implements Summary {
	private int count;
	private int duration;
	
	public SessionSummary() {
		this(0,  0);
	}
	
	public SessionSummary(int count, int duration) {
		this.count = count;
		this.duration = duration;
	}

	public int getCount() {
		return count;
	}
	
	public void setCount(int count) {
		this.count = count;
	}
	
	public int addCount(int count) {
		this.count += count;
		return this.count;
	}
	
	public int getDuration() {
		return duration;
	}

	public void setDuration(int duration) {
		this.duration = duration;
	}

	public void addDuration(int duration) {
		this.duration += duration;
	}
}
