package com.valuepotion.analytics;

import com.valuepotion.analytics.bases.LineBuilder;
import com.valuepotion.analytics.core.LineDataTool;


public class SessionLineBuilder extends LineBuilder<SessionSummary> {
	private SessionSummary total = new SessionSummary();
	
	private class Aggregation implements Filter<SessionSummary> {
		
		private SessionSummary summary = new SessionSummary();

		@Override
		public void add(SessionSummary ss) {
			int count = ss.getCount();
			int duration = ss.getDuration();

			summary.addCount(count);
			summary.addDuration(duration);

			total.addCount(count);
			total.addDuration(duration);
		}

		@Override
		public SessionSummary apply() {
			return summary;
		}
	}
	
	public SessionLineBuilder(LineDataTool dataTool) {
		super(new SessionSummarySerializer(), dataTool);
	}

	@Override
	public Filter<SessionSummary> initFilter() {
		return new Aggregation();
	}
	
	public SessionSummary getTotal() {
		return total;
	}

	@Override
	public SessionSummary[] get() {
		SessionSummary[] res = new SessionSummary[partitions.size()];
		for (int i = 0; i < partitions.size() ; i++) {
			Filter<SessionSummary> attribution = partitions.get(i);
			res[i] = attribution.apply();
		}
		
		return res;
	}
}
