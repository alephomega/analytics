package com.valuepotion.analytics;

import java.util.Arrays;
import java.util.List;

import com.valuepotion.analytics.aggregators.Aggregator;
import com.valuepotion.analytics.bases.Pair;
import com.valuepotion.analytics.events.Event;
import com.valuepotion.analytics.events.Purchase;

public class RevenueAggregator implements Aggregator<Pair<Integer, List<Event>>, PurchaseSummary[][]> {
	
	private PurchaseSummary[][] revenues;

	public RevenueAggregator(Integer d) {
		revenues = new PurchaseSummary[d+1][];
		Arrays.fill(revenues, new PurchaseSummary[0]);
	}
	
	@Override
	public void add(Pair<Integer, List<Event>> pair) {
		int r = pair.getX();
		List<Event> events = pair.getY();

		if (r < revenues.length) {
			revenues[r] = aggregate(revenues[r], events);
		}
	}
	
	private PurchaseSummary[] aggregate(PurchaseSummary[] prev, List<Event> purchases) {
		
		PurchaseSummaryAggregator aggregator = new PurchaseSummaryAggregator();
		for (Event event : purchases) {
			Purchase purchase = (Purchase) event;
			PurchaseSummary summary = new PurchaseSummary(purchase.getCurrency(), purchase.getCount(), purchase.getAmount());
			aggregator.add(new PurchaseSummary[] { summary });
		}
		
		aggregator.add(prev);

		return aggregator.get();
	}
	

	@Override
	public PurchaseSummary[][] get() {
		return revenues;
	}
}
