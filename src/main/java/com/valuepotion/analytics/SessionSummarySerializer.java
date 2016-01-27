package com.valuepotion.analytics;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.core.LineDataTool.FieldSeparator;
import com.valuepotion.analytics.serializers.Serializer;

public class SessionSummarySerializer implements Serializer<SessionSummary> {

	@Override
	public SessionSummary deserialize(String line, LineDataTool dataTool) {
		if (LineDataTool.isNA(line)) {
			return new SessionSummary();
		}
		
		String[] fields = LineDataTool.asFields(FieldSeparator.ELEMENTS, line);
		
		if (fields.length == 1) {
			return new SessionSummary(1, Integer.parseInt(fields[0]));
		} else {
			return new SessionSummary(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]));
		}
	}

	@Override
	public String serialize(SessionSummary t, LineDataTool dataTool) {
		if (t == null) {
			return StringUtils.EMPTY;
		}
		
		return LineDataTool.asLine(FieldSeparator.ELEMENTS, new Integer[] { t.getCount(), t.getDuration() });
	}
}
