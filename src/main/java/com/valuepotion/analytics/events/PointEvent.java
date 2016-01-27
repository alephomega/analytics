package com.valuepotion.analytics.events;


public abstract class PointEvent extends Event {

	private String time;

	public PointEvent(String time, String date) {
		super(date);
		this.time = time;
	}

	public String getTime() {
		return time;
	}
	
	
	@Override
	public int hashCode() {
		return time.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PointEvent) {
			PointEvent event = (PointEvent) obj;
			
			if (name().equals(event.name()) && time.equals(event.getTime())) {
				return true;
			}
		}
		
		return false;
	}
	
	@Override
	public int compareTo(Event o) {
		int cmp = name().compareTo(o.name());
		if (cmp == 0) {
			cmp = time.compareTo(((PointEvent) o).getTime());
		}
		
		return cmp;
	}
}
