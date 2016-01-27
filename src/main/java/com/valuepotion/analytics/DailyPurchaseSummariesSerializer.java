package com.valuepotion.analytics;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.core.LineDataTool.FieldSeparator;
import com.valuepotion.analytics.serializers.Serializer;

public class DailyPurchaseSummariesSerializer implements Serializer<PurchaseSummary[][]> {
	
	private PurchaseSummarySerializer serializer = new PurchaseSummarySerializer();
	
	@Override
	public PurchaseSummary[][] deserialize(String line, LineDataTool dataTool) {
		if (LineDataTool.isNA(line)) {
			return new PurchaseSummary[0][0];
		}
		
		String[] groups = LineDataTool.asFields(FieldSeparator.GROUPS_OF_ELEMENTS, line);
		
		PurchaseSummary[][] res = new PurchaseSummary[groups.length][];
		for (int i = 0; i < groups.length; i++) {
			res[i] = serializer.deserialize(groups[i], dataTool);
		}
		
		return res;
	}

	@Override
	public String serialize(PurchaseSummary[][] t, LineDataTool dataTool) {
		if (t == null) {
			return StringUtils.EMPTY;
		}
		
		String[] groups = new String[t.length];
		for (int i = 0; i < t.length; i++) {
			groups[i] = serializer.serialize(t[i], dataTool);
		}
		
		return LineDataTool.asLine(FieldSeparator.GROUPS_OF_ELEMENTS, groups);
	}
}
