package com.valuepotion.analytics;

import java.util.ArrayList;
import java.util.List;

import com.valuepotion.analytics.bases.CustomerEvent;
import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.core.LineDataTool.FieldSeparator;
import com.valuepotion.analytics.serializers.Serializer;

public class SessionsSerializer implements Serializer<List<List<CustomerEvent<SessionSummary>>>> {
	
	public static class SingleAttribution implements Serializer<List<CustomerEvent<SessionSummary>>> {

		@Override
		public List<CustomerEvent<SessionSummary>> deserialize(String line, LineDataTool dataTool) {
			List<CustomerEvent<SessionSummary>> res = new ArrayList<CustomerEvent<SessionSummary>>();

			String[] elements = LineDataTool.asFields(FieldSeparator.ELEMENTS, line);
			for (String element: elements) {

				String[] children = LineDataTool.asFields(FieldSeparator.CHILD_ELEMENTS, element);
				if (children.length > 0) {
					String date = dataTool.decodeDate(children[0]);
					String[] grandChildren = LineDataTool.asFields(FieldSeparator.GRANDCHILD_ELEMENTS, children[1]);
					int count = Integer.parseInt(grandChildren[0]);
					int duration = Integer.parseInt(grandChildren[1]);
					
					
					res.add(new CustomerEvent<SessionSummary>(date, new SessionSummary(count, duration)));
				}
			}
			
			return res;
		}
		
		@Override
		public String serialize(List<CustomerEvent<SessionSummary>> events, LineDataTool dataTool) {
			
			String[] elements = new String[events.size()];
			for (int j = 0; j < events.size(); j++) {
				CustomerEvent<SessionSummary> event = events.get(j);
				
				StringBuilder sb = new StringBuilder();
				SessionSummary summary = event.getSummary();
				if (summary.getCount() > 0) {
					sb.append(dataTool.encodeDate(event.getDate()));
					sb.append(FieldSeparator.CHILD_ELEMENTS.getSeparatorString());
					sb.append(LineDataTool.asLine(FieldSeparator.GRANDCHILD_ELEMENTS,
							new Integer[] { summary.getCount(), summary.getDuration() }));
				}
				
				elements[j] = sb.toString();
			}
				
			return LineDataTool.asLine(FieldSeparator.ELEMENTS, elements);
		}
	}

	
	private SingleAttribution serializer = new SingleAttribution();
	
	@Override
	public List<List<CustomerEvent<SessionSummary>>> deserialize(String line, LineDataTool dataTool) {
		String[] groups = LineDataTool.asFields(FieldSeparator.GROUPS_OF_ELEMENTS, line);
		
		List<List<CustomerEvent<SessionSummary>>> res = new ArrayList<List<CustomerEvent<SessionSummary>>>(groups.length);

		for (String group : groups) {
			res.add(serializer.deserialize(group, dataTool));
		}
		
		return res;
	}
	
	@Override
	public String serialize(List<List<CustomerEvent<SessionSummary>>> events, LineDataTool dataTool) {
		
		String[] groups = new String[events.size()];
		
		for (int i = 0; i < events.size(); i++) {
			groups[i] = serializer.serialize(events.get(i), dataTool);
		}
		
		return LineDataTool.asLine(FieldSeparator.GROUPS_OF_ELEMENTS, groups);
	}
}
