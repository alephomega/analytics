package com.valuepotion.analytics;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.core.LineDataTool.FieldSeparator;
import com.valuepotion.analytics.serializers.Serializer;

public class DailyUpdateSummariesSerializer implements Serializer<UpdateSummary[]> {
	private UpdateSummarySerializer serializer = new UpdateSummarySerializer();
	
	@Override
	public UpdateSummary[] deserialize(String line, LineDataTool dataTool) {
		if (LineDataTool.isNA(line)) {
			return new UpdateSummary[0];
		}
		
		String[] groups = LineDataTool.asFields(FieldSeparator.GROUPS_OF_ELEMENTS, line);
		UpdateSummary[] summaries = new UpdateSummary[groups.length];
		for (int i = 0; i < groups.length; i++) {
			summaries[i] = serializer.deserialize(groups[i], dataTool);
		}
		
		return summaries;
	}

	@Override
	public String serialize(UpdateSummary[] t, LineDataTool dataTool) {
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
