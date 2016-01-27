package com.valuepotion.analytics;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.valuepotion.analytics.bases.CustomerEvent;
import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.core.LineDataTool.FieldSeparator;
import com.valuepotion.analytics.serializers.Serializer;

public class PurchasesSerializer implements Serializer<List<List<CustomerEvent<PurchaseSummary[]>>>> {
	
	public static class SingleAttribution implements Serializer<List<CustomerEvent<PurchaseSummary[]>>> {

		@Override
		public List<CustomerEvent<PurchaseSummary[]>> deserialize(String line, LineDataTool dataTool) {
			String[] elements = LineDataTool.asFields(FieldSeparator.ELEMENTS, line);

			List<CustomerEvent<PurchaseSummary[]>> res = new ArrayList<CustomerEvent<PurchaseSummary[]>>(elements.length);
			for (String element: elements) {

				String[] children = LineDataTool.asFields(FieldSeparator.CHILD_ELEMENTS, element);
				if (children.length > 0) {
					PurchaseSummary[] summary = new PurchaseSummary[children.length - 1];
					
					String date = dataTool.decodeDate(children[0]);
					for (int i = 1; i < children.length; i++) {
						String[] grandChildren = LineDataTool.asFields(FieldSeparator.GRANDCHILD_ELEMENTS, children[i]);
						
						String currency = grandChildren[0];
						int count = Integer.parseInt(grandChildren[1]);
						double amount = Double.parseDouble(grandChildren[2]);
						
						summary[i - 1] = new PurchaseSummary(currency, count, amount);
					}
					
					res.add(new CustomerEvent<PurchaseSummary[]>(date, summary));
				}
			}
		
			return res;
		}

		@Override
		public String serialize(List<CustomerEvent<PurchaseSummary[]>> events, LineDataTool dataTool) {
			
			String[] elements = new String[events.size()];
			for (int j = 0; j < events.size(); j++) {
				CustomerEvent<PurchaseSummary[]> event = events.get(j);
				if (event.getSummary().length == 0) {
					elements[j] = StringUtils.EMPTY;
					
				} else {
					StringBuilder sb = new StringBuilder(dataTool.encodeDate(event.getDate()));
					PurchaseSummary[] summary = event.getSummary();
					for (int k = 0; k < summary.length; k++) {
						sb.append(FieldSeparator.CHILD_ELEMENTS.getSeparatorString());
						sb.append(LineDataTool.asLine(FieldSeparator.GRANDCHILD_ELEMENTS,
								new Object[] { 
								summary[k].getCurrency(), summary[k].getCount(), summary[k].getAmount() }));
					}
					
					elements[j] = sb.toString();
				}
			}
			
			return LineDataTool.asLine(FieldSeparator.ELEMENTS, elements);
		}
	}
	

	private SingleAttribution serializer = new SingleAttribution();
	
	@Override
	public List<List<CustomerEvent<PurchaseSummary[]>>> deserialize(String line, LineDataTool dataTool) {
		String[] groups = LineDataTool.asFields(FieldSeparator.GROUPS_OF_ELEMENTS, line);
		
		List<List<CustomerEvent<PurchaseSummary[]>>> res = new LinkedList<List<CustomerEvent<PurchaseSummary[]>>>();
		for (String group : groups) {
			res.add(serializer.deserialize(group, dataTool));
		}
		
		return res;
	}

	@Override
	public String serialize(List<List<CustomerEvent<PurchaseSummary[]>>> events, LineDataTool dataTool) {
		String[] groups = new String[events.size()];
		
		for (int i = 0; i < events.size(); i++) {
			groups[i] = serializer.serialize(events.get(i), dataTool);
		}
		
		return LineDataTool.asLine(FieldSeparator.GROUPS_OF_ELEMENTS, groups);
	}
}
