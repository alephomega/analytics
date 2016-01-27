package com.valuepotion.analytics;

import java.io.IOException;
import java.util.Calendar;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.valuepotion.analytics.aggregators.Aggregator;
import com.valuepotion.analytics.bases.Timeline;
import com.valuepotion.analytics.core.AnalyticsDriver;
import com.valuepotion.analytics.core.AnalyticsMapper;
import com.valuepotion.analytics.core.AnalyticsReducer;
import com.valuepotion.analytics.core.CombineKeyValueTextInputFormat;
import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.events.Event;
import com.valuepotion.analytics.events.Session;

public class PeriodicAnalytics extends AnalyticsDriver {
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new PeriodicAnalytics(), args);
	}
	
	
	@Override
	public Job prepareJob(Configuration conf, String input, String output) throws IOException {
		
		return prepareJob(
				conf,
				"valuepotion.analytics.periodic-statistics",
				input,
				output,
				CombineKeyValueTextInputFormat.class, 
				PeriodicAnalyticsMapper.class, 
				Text.class, 
				Text.class, 
				PeriodicAnalyticsReducer.class, 
				Text.class, 
				Text.class,
				PeriodicAnalyticsReducer.class,
				null, 
				null,
				null,
				TextOutputFormat.class);
	}
	
	
	static class PeriodicAnalyticsMapper extends AnalyticsMapper<Text, Text, Text, Text> {
		private String firstDay;
		private String monday;

		private Text k = new Text();
		private Text v = new Text();
		
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			
			this.monday = getMonday();
			this.firstDay = get1stDay();
		}

		private String get1stDay() {
			Calendar calendar = Calendar.getInstance();
			calendar.setTimeInMillis(dataTool.baseTime());
			calendar.add(Calendar.DATE, 1 - calendar.get(Calendar.DATE));
			return dataTool.asDateString(calendar.getTimeInMillis());
		}

		private String getMonday() {
			Calendar calendar = Calendar.getInstance();
			calendar.setTimeInMillis(dataTool.baseTime());

			int d = 0;
			switch(calendar.get(Calendar.DAY_OF_WEEK)) {
			case Calendar.SUNDAY:
				d = -6;
				break;
				
			case Calendar.MONDAY:
				d = 0;
				break;
				
			case Calendar.TUESDAY:
				d = -1;
				break;
				
			case Calendar.WEDNESDAY:
				d = -2;
				break;
				
			case Calendar.THURSDAY:
				d = -3;
				break;
				
			case Calendar.FRIDAY:
				d = -4;
				break;
				
			case Calendar.SATURDAY:
				d = -5;
				break;
			}
			
			calendar.add(Calendar.DATE, d);
			return dataTool.asDateString(calendar.getTimeInMillis());
		}
		
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			k.set(LineDataTool.asFields(key.toString())[0]);
			
			String[] attributes = Attributes.asFields(value.toString());
			Timeline timeline = Attributes.getTimeline(attributes, dataTool);
			
			int wau = 0;
			List<Event> sessions = timeline.getEvents(new Session(monday), new Session(dataTool.baseDate()));
			if (!sessions.isEmpty()) {
				wau = 1;
			}
			
			int mau = 0;
			sessions = timeline.getEvents(new Session(firstDay), new Session(dataTool.baseDate()));
			if (!sessions.isEmpty()) {
				mau = 1;
			}
			
			if (mau > 0 || wau > 0) {
				String[] statistics = PeriodicStatistics.init();
				PeriodicStatistics.MAU.set(statistics, Integer.valueOf(mau), dataTool);
				PeriodicStatistics.WAU.set(statistics, Integer.valueOf(wau), dataTool);
				
				v.set(PeriodicStatistics.asLine(statistics));
				context.write(k, v);
			}
		}
	}
	
	static class PeriodicAnalyticsReducer extends AnalyticsReducer<Text, Text, Text, Text> {
		private Text analytics = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Aggregator[] aggregators;
			try {
				aggregators = PeriodicStatistics.createAggregators();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			for (Text value : values) {
				String[] statistics = PeriodicStatistics.asFields(value.toString());
				
				PeriodicStatistics.WAU.add(PeriodicStatistics.WAU.get(statistics, dataTool), aggregators);
				PeriodicStatistics.MAU.add(PeriodicStatistics.MAU.get(statistics, dataTool), aggregators);
			}
			
			analytics.set(LineDataTool.asLine(PeriodicStatistics.asLines(aggregators, dataTool)));
			context.write(key, analytics);
		}
	}
}
