package com.valuepotion.analytics.bases;

public class CustomerEvent<S> {
	
	private String date;
	private S summary;
	
	public CustomerEvent(String date, S summary) {
		this.date = date;
		this.summary = summary;
	}

	public String getDate() {
		return date;
	}
	
	public void setDate(String date) {
		this.date = date;
	}
	
	public S getSummary() {
		return summary;
	}
	
	public void setSummary(S summary) {
		this.summary = summary;
	}
}
