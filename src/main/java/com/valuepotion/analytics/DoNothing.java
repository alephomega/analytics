package com.valuepotion.analytics;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.valuepotion.analytics.core.AnalyticsDriver;
import com.valuepotion.analytics.core.CombineKeyValueTextInputFormat;

public class DoNothing extends AnalyticsDriver {
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DoNothing(), args);
	}

	@Override
	public Job prepareJob(Configuration conf, String input, String output)
			throws IOException {
		
		return prepareJob(
				conf,
				"valuepotion.analytics.balancer",
				input,
				output,
				CombineKeyValueTextInputFormat.class, 
				Mapper.class, 
				Text.class, 
				Text.class, 
				Reducer.class, 
				Text.class, 
				Text.class,
				null,
				null, 
				null,
				null,
				TextOutputFormat.class);
	}
}
