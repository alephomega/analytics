package com.valuepotion.analytics.events;

import com.valuepotion.analytics.bases.IntervalEvent;

public class Purchase extends IntervalEvent {
	private String currency;
	private double amount;
	
	public Purchase(String date) {
		super(date);
	}
	
	public Purchase(String date, String currency, int count, double amount) {
		super(date, count);
		this.currency = currency;
		this.amount = amount;
	}

	public String getCurrency() {
		return currency;
	}

	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}

	@Override
	public String name() {
		return "purchase";
	}
	
	@Override
	public int hashCode() {
		return (getDate() + currency).hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Purchase) {
			Purchase event = (Purchase) obj;
			
			if (name().equals(event.name()) && getDate().equals(event.getDate()) && currency.equals(event.getCurrency())) {
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
