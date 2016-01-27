package com.valuepotion.analytics;

public class UpdateSummary {
	private int count;

	public UpdateSummary() {
		this(0);
	}

	public UpdateSummary(int count) {
		this.count = count;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
	public void addCount(int count) {
		this.count += count;
	}
}
