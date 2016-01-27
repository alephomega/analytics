package com.valuepotion.analytics;

import com.valuepotion.analytics.bases.LineBuilder;
import com.valuepotion.analytics.core.LineDataTool;


public class UpdateLineBuilder extends LineBuilder<UpdateSummary> {
	private UpdateSummary summary = new UpdateSummary();
	
	private class Aggregation implements Filter<UpdateSummary> {

		@Override
		public void add(UpdateSummary s) {
			summary.addCount(s.getCount());
		}

		@Override
		public UpdateSummary apply() {
			return summary;
		}
	}
	
	public UpdateLineBuilder(LineDataTool dataTool) {
		super(new UpdateSummarySerializer(), dataTool);
	}

	@Override
	public Filter<UpdateSummary> initFilter() {
		return new Aggregation();
	}
	
	@Override
	public UpdateSummary[] get() {
		UpdateSummary[] res = new UpdateSummary[partitions.size()];
		for (int i = 0; i < partitions.size() ; i++) {
			Filter<UpdateSummary> attribution = partitions.get(i);
			res[i] = attribution.apply();
		}
		
		return res;
	}
}
