package com.valuepotion.analytics.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

public class AnalyticsReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	protected LineDataTool dataTool;
	
	@Override
	protected void setup(Context context) {
		Configuration conf = context.getConfiguration();
		this.dataTool = new LineDataTool(conf);
	}
}
