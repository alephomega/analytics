package com.valuepotion.analytics.core;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hcatalog.common.HCatException;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatInputFormat;

public class HiveAnalyticsMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends AnalyticsMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	private HCatSchema tableSchema;
	private String[] columns;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		try {
			this.tableSchema = HCatInputFormat.getTableSchema(conf);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		this.columns = conf.getStrings("valuepotion.analytics.columns");
	}
	
	
	protected String[] getFields(HCatRecord record) {
		String[] values = new String[columns.length];
		
		for (int i = 0; i < columns.length; i++) {
			try {
				values[i] = String.valueOf(record.get(columns[i], tableSchema));
				if (values[i] == null || values[i].equals("null")) {
					values[i] = StringUtils.EMPTY;
				}
			} catch (HCatException e) {
				throw new RuntimeException(e);
			}
		}

		return values;
	}
}