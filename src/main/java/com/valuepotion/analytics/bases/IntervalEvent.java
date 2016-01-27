package com.valuepotion.analytics.bases;

import com.valuepotion.analytics.events.Event;

public abstract class IntervalEvent extends Event {
	private int count;

	public IntervalEvent(String date) {
		this(date, 0);
	}

	public IntervalEvent(String date, int count) {
		super(date);
		this.count = count;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof IntervalEvent) {
			IntervalEvent event = (IntervalEvent) obj;
			
			if (name().equals(event.name()) && getDate().equals(event.getDate())) {
				return true;
			}
		}
		
		return false;
	}

	@Override
	public int compareTo(Event o) {
		int cmp = name().compareTo(o.name());
		if (cmp == 0) {
			cmp = getDate().compareTo(((IntervalEvent) o).getDate());
		}
		
		return cmp;
	}
}
