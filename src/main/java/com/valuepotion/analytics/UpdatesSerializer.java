package com.valuepotion.analytics;

import java.util.ArrayList;
import java.util.List;

import com.valuepotion.analytics.bases.CustomerEvent;
import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.core.LineDataTool.FieldSeparator;
import com.valuepotion.analytics.serializers.Serializer;

public class UpdatesSerializer implements Serializer<List<List<CustomerEvent<UpdateSummary>>>> {

	public static class SingleAttribution implements Serializer<List<CustomerEvent<UpdateSummary>>> {

		@Override
		public List<CustomerEvent<UpdateSummary>> deserialize(String line, LineDataTool dataTool) {
			List<CustomerEvent<UpdateSummary>> res = new ArrayList<CustomerEvent<UpdateSummary>>();

			String[] elements = LineDataTool.asFields(FieldSeparator.ELEMENTS, line);
			for (String element: elements) {

				String[] children = LineDataTool.asFields(FieldSeparator.CHILD_ELEMENTS, element);
				if (children.length > 0) {
					String date = dataTool.decodeDate(children[0]);
					int count = Integer.parseInt(children[1]);
					
					res.add(new CustomerEvent<UpdateSummary>(date, new UpdateSummary(count)));
				}
			}
			
			return res;
		}
		
		@Override
		public String serialize(List<CustomerEvent<UpdateSummary>> events, LineDataTool dataTool) {
			
			String[] elements = new String[events.size()];
			for (int j = 0; j < events.size(); j++) {
				CustomerEvent<UpdateSummary> event = events.get(j);
				
				StringBuilder sb = new StringBuilder();
				UpdateSummary summary = event.getSummary();
				if (summary.getCount() > 0) {
					sb.append(dataTool.encodeDate(event.getDate()));
					sb.append(FieldSeparator.CHILD_ELEMENTS.getSeparatorString());
					sb.append(summary.getCount());
				}
				
				elements[j] = sb.toString();
			}
				
			return LineDataTool.asLine(FieldSeparator.ELEMENTS, elements);
		}
	}

	
	private SingleAttribution serializer = new SingleAttribution();
	
	@Override
	public List<List<CustomerEvent<UpdateSummary>>> deserialize(String line, LineDataTool dataTool) {
		String[] groups = LineDataTool.asFields(FieldSeparator.GROUPS_OF_ELEMENTS, line);
		
		List<List<CustomerEvent<UpdateSummary>>> res = new ArrayList<List<CustomerEvent<UpdateSummary>>>(groups.length);

		for (String group : groups) {
			res.add(serializer.deserialize(group, dataTool));
		}
		
		return res;
	}
	
	@Override
	public String serialize(List<List<CustomerEvent<UpdateSummary>>> events, LineDataTool dataTool) {
		
		String[] groups = new String[events.size()];
		
		for (int i = 0; i < events.size(); i++) {
			groups[i] = serializer.serialize(events.get(i), dataTool);
		}
		
		return LineDataTool.asLine(FieldSeparator.GROUPS_OF_ELEMENTS, groups);
	}
}
