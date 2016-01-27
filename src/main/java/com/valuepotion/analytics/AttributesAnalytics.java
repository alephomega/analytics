package com.valuepotion.analytics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli2.Option;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.valuepotion.analytics.aggregators.Aggregator;
import com.valuepotion.analytics.bases.CustomerEvent;
import com.valuepotion.analytics.core.AnalyticsDriver;
import com.valuepotion.analytics.core.AnalyticsReducer;
import com.valuepotion.analytics.core.CombineKeyValueTextInputFormat;
import com.valuepotion.analytics.core.CommandLineOptionTool;
import com.valuepotion.analytics.core.LineDataTool;


public class AttributesAnalytics extends AnalyticsDriver {
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new AttributesAnalytics(), args);
	}
	
	@Override
	protected void addOptions() {
		super.addOptions();
		
		optionTool.addOption(riskOption());
		optionTool.addOption(defectionOption());
	}

	@Override
	public Job prepareJob(Configuration conf, String input, String output) throws IOException {
		Job job = prepareJob(
				conf,
				"valuepotion.analytics.customer-attributes",
				input,
				output,
				CombineKeyValueTextInputFormat.class, 
				Mapper.class, 
				Text.class, 
				Text.class, 
				AttributesReducer.class, 
				Text.class, 
				Text.class,
				null, 
				null, 
				null,
				null,
				TextOutputFormat.class);

		for (SessionLifecycle lifecycle : SessionLifecycle.values()) {
			MultipleOutputs.addNamedOutput(job, basename(lifecycle), TextOutputFormat.class, Text.class, Text.class);
		}
		
		return job;
	}
	
	public static String basename(SessionLifecycle lifecycle) {
		return StringUtils.replace(lifecycle.toString(), "_", "");
	}
	
	private static Option riskOption() {
		return CommandLineOptionTool.buildOption(
				"risk",
				"r", 
				"The minimum recency (days) that a customer is regarded as being at risk.", 
				true, 
				false, 
				null);
	}

	private static Option defectionOption() {
		return CommandLineOptionTool.buildOption(
				"defection",
				"d", 
				"The minimum recency (days) that a customer is regarded as being lost.", 
				true, 
				false, 
				null);
	}


	
	static class AttributesReducer extends AnalyticsReducer<Text, Text, Text, Text> {
		private int defection;
		private int lisk;
		
		private MultipleOutputs<Text, Text> outputs;
		private Text value = new Text();

		
		@Override
		public void setup(Context context) {
			super.setup(context);

			Configuration conf = context.getConfiguration();
			defection = Integer.parseInt(conf.get("valuepotion.analytics.defection", "365"));
			lisk = Integer.parseInt(conf.get("valuepotion.analytics.lisk", "30"));
			outputs = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] attributes = null;
			String[] summary = null;
			
			String[] keys = LineDataTool.asFields(key.toString());
			if (LineDataTool.isNA(keys[1])) {
				return;
			}
			
			for (Text value : values) {
				String line = value.toString();

				switch(Type.fromSymbol(line)) {
				
				case DAILY_SUMMARY: 
					summary = DailySummary.asFields(line);
					break;
					
				case ATTRIBUTES:
					attributes = Attributes.asFields(line);
					break;
					
				case ATTRIBUTIONS:
				case ILLEGAL:
					throw new RuntimeException(String.format("Value must start with '~' or '*': (%s, %s)", key.toString(), line));
				}
			}
			
			SessionLifecycle lifecycle = SessionLifecycle.ACTIVE;
			String record = null;
			
			if (attributes == null) {
				attributes = ds2a(summary);
				
				int installationCount = (Integer) Attributes.INSTALLATION_COUNT.get(attributes, dataTool);
				if (installationCount == 0) {
					lifecycle = SessionLifecycle.UNKNOWN;
				} else {
					lifecycle = SessionLifecycle.NEW;
				}
				
				record = Attributes.asLine(attributes);

			} else {
				int r = recency(attributes);
				
				boolean lost = false;
				boolean atLisk = false;

				if (r > defection) {
					lost = true;
				} else if (r > lisk) {
					atLisk = true;
				}
				
				if (summary == null) {
					record = Attributes.asLine(attributes);
					
					if (lost) {
						lifecycle = SessionLifecycle.LOST;
					} else if (atLisk) {
						lifecycle = SessionLifecycle.AT_RISK;
					}
					
				} else {
					try {
						attributes = merge(attributes, summary);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
					
					record = Attributes.asLine(attributes);
					
					if (atLisk) {
						lifecycle = SessionLifecycle.WINBACK;
					}
				}
			}
			
			value.set(record);
			classify(lifecycle, key, value);

			if (summary != null) {
				value.set(Attributes.asLine(attributes));
				context.write(key, value);
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			outputs.close();
		}

		
		String[] ds2a(String[] summary) {
			String[] attributes = Attributes.init();
			
			Attributes.INSTALLATION_COUNT.set(
					attributes, 
					DailySummary.INSTALLATION_COUNT.get(summary, dataTool),
					dataTool);
			
			Attributes.SESSION_COUNT.set(
					attributes, 
					DailySummary.SESSION_COUNT.get(summary, dataTool),
					dataTool);
			
			Attributes.SESSION_DURATION.set(
					attributes, 
					DailySummary.SESSION_DURATION.get(summary, dataTool),
					dataTool);
			
			Attributes.FIRST_SESSION_TIME.set(
					attributes, 
					DailySummary.FIRST_SESSION_TIME.get(summary, dataTool),
					dataTool);
			
			Attributes.FIRST_SESSION_DATE.set(
					attributes, 
					dataTool.baseDate(),
					dataTool);
			
			Attributes.INSTALLATION_DATES.set(
					attributes, 
					Attributes.fields(dataTool.baseDate(), ((String[]) DailySummary.INSTALLATION_TIMES.get(summary, dataTool)).length),
					dataTool);
			
			Attributes.INSTALLATION_TIMES.set(
					attributes, 
					DailySummary.INSTALLATION_TIMES.get(summary, dataTool),
					dataTool);
			
			Attributes.ATTRIBUTIONS.set(
					attributes, 
					DailySummary.ATTRIBUTIONS.get(summary, dataTool),
					dataTool);
			
			Attributes.SESSIONS.set(
					attributes, 
					toEventsHistory((SessionSummary[]) DailySummary.SESSIONS.get(summary, dataTool), dataTool),
					dataTool);
			
			Attributes.PURCHASES.set(
					attributes, 
					toEventsHistory((PurchaseSummary[][]) DailySummary.PURCHASES.get(summary, dataTool), dataTool),
					dataTool);

			Attributes.UPDATES.set(
					attributes, 
					toEventsHistory((UpdateSummary[]) DailySummary.UPDATES.get(summary, dataTool), dataTool),
					dataTool);
			
			Attributes.OS_VERSIONS.set(
					attributes, 
					DailySummary.OS_VERSIONS.get(summary, dataTool),
					dataTool);
			
			Attributes.APP_VERSIONS.set(
					attributes, 
					DailySummary.APP_VERSIONS.get(summary, dataTool),
					dataTool);
			
			Attributes.DEVICE_MODELS.set(
					attributes, 
					DailySummary.DEVICE_MODELS.get(summary, dataTool),
					dataTool);
			
			Attributes.COUNTRIES.set(
					attributes, 
					DailySummary.COUNTRIES.get(summary, dataTool),
					dataTool);
			
			Attributes.BIRTH_YEARS.set(
					attributes, 
					DailySummary.BIRTH_YEARS.get(summary, dataTool),
					dataTool);
			
			Attributes.GENDERS.set(
					attributes, 
					DailySummary.GENDERS.get(summary, dataTool),
					dataTool);
			
			Attributes.LEVELS.set(
					attributes, 
					DailySummary.LEVELS.get(summary, dataTool),
					dataTool);
			
			return attributes;
		}
		
		
		
		private static <S> List<List<CustomerEvent<S>>> toEventsHistory(S[] summaries, LineDataTool dataTool) {
			List<List<CustomerEvent<S>>> res = new ArrayList<List<CustomerEvent<S>>>();
			if (summaries.length > 0) {
				res.add(new ArrayList<CustomerEvent<S>>());

				List<CustomerEvent<S>> group = res.get(res.size() - 1);
				group.add(new CustomerEvent<S>(dataTool.baseDate(), summaries[0]));
				
				for (int i = 1; i < summaries.length; i++) {
					group = new ArrayList<CustomerEvent<S>>();
					group.add(new CustomerEvent<S>(dataTool.baseDate(), summaries[i]));
					res.add(group);
				}
			}
			
			return res;
		}
		
		
		private String[] merge(String[] a, String[] s) throws Exception {
			String[] da = ds2a(s);
			
			Aggregator[] aggregators = Attributes.aggregators();
			
			Attributes[] values = Attributes.values();
			for (Attributes value : values) {
				value.add(value.get(a, dataTool), aggregators, dataTool);
				value.add(value.get(da, dataTool), aggregators, dataTool);
				
				value.set(a, value.get(aggregators), dataTool);
			}
			
			return a;
		}
		
		/*
		private boolean zzz(String[] attributes) {
			return !(recency(attributes) <= defection);
		}
		*/

		private int recency(String[] attributes) {
			String line = Attributes.SESSIONS.getString(attributes, dataTool);
			EventHistory<SessionSummary> history = new EventHistory(line, Attributes.SESSIONS.getSerializer(), dataTool);
			
			CustomerEvent<SessionSummary> lastSession = history.lastEvent();
			
			int r;
			if (lastSession == null) {
				r = dataTool.diffDays((String) Attributes.FIRST_SESSION_DATE.get(attributes, dataTool));
			} else {
				r = dataTool.diffDays(lastSession.getDate());
			}
			
			return r;
		}
		
		private void classify(SessionLifecycle lifecycle, Text key, Text value) throws IOException, InterruptedException {
			outputs.write(basename(lifecycle), key, value);
		}
	}
}