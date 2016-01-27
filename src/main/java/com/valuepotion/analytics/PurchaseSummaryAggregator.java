package com.valuepotion.analytics;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.valuepotion.analytics.aggregators.Aggregator;

public class PurchaseSummaryAggregator implements Aggregator<PurchaseSummary[], PurchaseSummary[]>{
	private Map<String, PurchaseSummary> aggregation = new HashMap<String, PurchaseSummary>();

	@Override
	public void add(PurchaseSummary[] summaries) {
		
		for (PurchaseSummary summary : summaries) {
			String currency = summary.getCurrency();
			int count = summary.getCount();
			double amount = summary.getAmount();
			
			PurchaseSummary av = aggregation.get(currency);
			if (av == null) {
				av = new PurchaseSummary(currency, count, amount);
				
				aggregation.put(currency, av);
			} else {
				av.addCount(count);
				av.addAmount(amount);
			}
		}
	}

	@Override
	public PurchaseSummary[] get() {
		
		PurchaseSummary[] summaries = new PurchaseSummary[aggregation.size()];
		Iterator<PurchaseSummary> iterator = aggregation.values().iterator();
		
		int i = 0;
		while (iterator.hasNext()) {
			summaries[i++] = (PurchaseSummary) iterator.next();
		}
		
		return summaries;
	}

	public void reset() {
		aggregation.clear();
	}
}
