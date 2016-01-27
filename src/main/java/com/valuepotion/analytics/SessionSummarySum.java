package com.valuepotion.analytics;

import com.valuepotion.analytics.aggregators.Aggregator;

public class SessionSummarySum implements Aggregator<SessionSummary, SessionSummary> {
	
	private SessionSummary summary = new SessionSummary();

	@Override
	public void add(SessionSummary s) {
		summary.addCount(s.getCount());
		summary.addDuration(s.getDuration());
	}

	@Override
	public SessionSummary get() {
		return summary;
	}
}
