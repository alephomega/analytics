package com.valuepotion.analytics;

import java.util.HashMap;
import java.util.Map;

import com.valuepotion.analytics.bases.LineBuilder;
import com.valuepotion.analytics.core.LineDataTool;

public class PurchaseLineBuilder extends LineBuilder<PurchaseSummary[]> {
	
	private class Aggregation implements Filter<PurchaseSummary[]> {
		
		private Map<String, PurchaseSummary> aggregation = new HashMap<String, PurchaseSummary>();

		@Override
		public void add(PurchaseSummary[] summaries) {
			
			for (int i = 0; i < summaries.length; i++) {
				
				PurchaseSummary purchase = (PurchaseSummary) aggregation.get(summaries[i].getCurrency());
				if (purchase == null) {
					purchase = summaries[i];
					
					aggregation.put(summaries[i].getCurrency(), purchase);
				} else {
					purchase.addCount(1);
					purchase.addAmount(summaries[i].getAmount());
				}
			}
		}

		@Override
		public PurchaseSummary[] apply() {
			if (aggregation.size() == 0) {
				return new PurchaseSummary[0];
			}

			PurchaseSummary[] res = new PurchaseSummary[aggregation.size()];

			int i = 0;
			for (Map.Entry<String, PurchaseSummary> entry : aggregation.entrySet()) {
				res[i++] = entry.getValue();
			}

			return res;
		}
	}
	
	public PurchaseLineBuilder(LineDataTool dataTool) {
		super(new PurchaseSummarySerializer(), dataTool);
	}

	@Override
	public Filter<PurchaseSummary[]> initFilter() {
		return new Aggregation();
	}
	
	@Override
	public PurchaseSummary[][] get() {
		PurchaseSummary[][] res = new PurchaseSummary[partitions.size()][];
		for (int i = 0; i < partitions.size() ; i++) {
			Filter<PurchaseSummary[]> attribution = partitions.get(i);
			res[i] = attribution.apply();
		}
		
		return res;
	}
}
