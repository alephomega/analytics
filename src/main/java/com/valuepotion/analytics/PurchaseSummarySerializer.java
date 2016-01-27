package com.valuepotion.analytics;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.core.LineDataTool.FieldSeparator;
import com.valuepotion.analytics.serializers.Serializer;

public class PurchaseSummarySerializer implements Serializer<PurchaseSummary[]> {

	@Override
	public PurchaseSummary[] deserialize(String line, LineDataTool dataTool) {
		if (LineDataTool.isNA(line)) {
			return new PurchaseSummary[0];
		}
		
		String[] elements = LineDataTool.asFields(FieldSeparator.ELEMENTS, line);
		PurchaseSummary[] summaries = new PurchaseSummary[elements.length];
			
		for (int j = 0; j < elements.length; j++) {
			String[] children = LineDataTool.asFields(FieldSeparator.CHILD_ELEMENTS, elements[j]);
			
			if (children.length == 2) {
				summaries[j] = new PurchaseSummary(children[0], 1, Double.parseDouble(children[1]));
			} else {
				summaries[j] = new PurchaseSummary(children[0], Integer.parseInt(children[1]), Double.parseDouble(children[2]));
			}
		}
		
		return summaries;
	}

	@Override
	public String serialize(PurchaseSummary[] t, LineDataTool dataTool) {
		if (t == null) {
			return StringUtils.EMPTY;
		}
		
		String[] elements = new String[t.length];
		for (int i = 0; i < t.length; i++) {
			if (t[i] == null) {
				elements[i] = StringUtils.EMPTY;
				continue;
			}
			
			elements[i] = LineDataTool.asLine(FieldSeparator.CHILD_ELEMENTS, new Object[] { t[i].getCurrency(), t[i].getCount(), t[i].getAmount() });
		}
		
		return LineDataTool.asLine(FieldSeparator.ELEMENTS, elements);
	}
}
