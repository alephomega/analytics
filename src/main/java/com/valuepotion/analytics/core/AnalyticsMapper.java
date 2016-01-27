package com.valuepotion.analytics.core;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

public class AnalyticsMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	protected LineDataTool dataTool;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		Configuration conf = context.getConfiguration();
		this.dataTool = new LineDataTool(conf);
	}
}
