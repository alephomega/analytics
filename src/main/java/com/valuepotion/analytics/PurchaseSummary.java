package com.valuepotion.analytics;

import com.valuepotion.analytics.bases.Summary;

public class PurchaseSummary implements Summary {
	
	private String currency;
	private int count;
	private double amount;
	
	public PurchaseSummary(String currency, int count, double amount) {
		this.currency = currency;
		this.count = count;
		this.amount = amount;
	}

	public String getCurrency() {
		return currency;
	}
	
	public int getCount() {
		return count;
	}
	
	public double getAmount() {
		return amount;
	}
	
	public void setCurrency(String currency) {
		this.currency = currency;
	}

	public void setCount(int count) {
		this.count = count;
	}
	
	public void addCount(int count) {
		this.count += count;
	}
	
	public void addAmount(double amount) {
		this.amount += amount;
	}
	
	public void setAmount(double amount) {
		this.amount = amount;
	}
}
