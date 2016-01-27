package com.valuepotion.analytics;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.serializers.Serializer;

public class UpdateSummarySerializer implements Serializer<UpdateSummary>{

	@Override
	public UpdateSummary deserialize(String line, LineDataTool dataTool) {
		if (LineDataTool.isNA(line)) {
			return new UpdateSummary();
		} else {
			return new UpdateSummary(Integer.parseInt(line));
		}
	}

	@Override
	public String serialize(UpdateSummary t, LineDataTool dataTool) {
		if (t == null) {
			return StringUtils.EMPTY;
		}
		
		return Integer.toString(t.getCount());
	}

}
