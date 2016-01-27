package com.valuepotion.analytics;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import com.valuepotion.analytics.aggregators.Aggregator;
import com.valuepotion.analytics.bases.Pair;
import com.valuepotion.analytics.bases.Timeline;
import com.valuepotion.analytics.core.AnalyticsDriver;
import com.valuepotion.analytics.core.AnalyticsMapper;
import com.valuepotion.analytics.core.AnalyticsReducer;
import com.valuepotion.analytics.core.CombineKeyValueTextInputFormat;
import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.events.Event;
import com.valuepotion.analytics.events.Purchase;
import com.valuepotion.analytics.events.Session;
import com.valuepotion.analytics.events.Update;

public class DailyAnalytics extends AnalyticsDriver {

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DailyAnalytics(), args);
	}

	
	@Override
	public Job prepareJob(Configuration conf, String input, String output)
			throws IOException {
		
		return prepareJob(
				conf,
				"valuepotion.analytics.daily-statistics",
				input,
				output,
				CombineKeyValueTextInputFormat.class, 
				DailyAnalyticsMapper.class, 
				Text.class, 
				Text.class, 
				DailyAnalyticsReducer.class, 
				Text.class, 
				Text.class,
				null,
				null, 
				null,
				null,
				TextOutputFormat.class);
	}
	

	static class DailyAnalyticsMapper extends AnalyticsMapper<Text, Text, Text, Text> {

		private Text k = new Text();
		
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			k.set(LineDataTool.asFields(key.toString())[0]);
			context.write(k, value);
		}
	}
	
	static class DailyAnalyticsReducer extends AnalyticsReducer<Text, Text, Text, Text> {
		private Text analytics = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Aggregator[] aggregators;
			try {
				aggregators = DailyStatistics.createAggregators();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			for (Text value : values) {
				String[] attributes = Attributes.asFields(value.toString());
				
				String firstSessionDate = (String) Attributes.FIRST_SESSION_DATE.get(attributes, dataTool);
				String[] installationDates = (String[]) Attributes.INSTALLATION_DATES.get(attributes, dataTool);
				if (installationDates.length > 0) {
					
					if (dataTool.diffDays(installationDates[installationDates.length - 1]) == 0) {
						if (dataTool.diffDays(firstSessionDate) > 0) {
							DailyStatistics.REINSTALLATION_COUNT.add(1, aggregators);
							
						} else {
							DailyStatistics.INSTALLATION_COUNT.add(1, aggregators);
						}
					}
				}
				
				DailyStatistics.DAU.add(1, aggregators);

				Timeline timeline = Attributes.getTimeline(attributes, dataTool);
				
				List<Event> sessions = timeline.getEvents(new Session(dataTool.baseDate()), new Session(dataTool.baseDate()));
				for (Event event : sessions) {
					Session session = (Session) event;
					DailyStatistics.SESSIONS.add(new SessionSummary(session.getCount(), session.getDuration()), aggregators);
				}
			
				List<Event> purchases = timeline.getEvents(new Purchase(dataTool.baseDate()), new Purchase(dataTool.baseDate()));
				for (Event event : purchases) {
					Purchase purchase = (Purchase) event;
					DailyStatistics.PURCHASES.add(new PurchaseSummary[] { new PurchaseSummary(purchase.getCurrency(), purchase.getCount(), purchase.getAmount()) }, aggregators);
				}
				
				if (!purchases.isEmpty()) {
					DailyStatistics.PAYING_USERS.add(1, aggregators);
				}

				List<Event> updates = timeline.getEvents(new Update(dataTool.baseDate()), new Update(dataTool.baseDate()));
				for (Event event : updates) {
					Update update = (Update) event;
					
					if (update.getCount() > 0) {
						DailyStatistics.UPDATE_COUNT.add(1, aggregators);
						break;
					}
				}
			
				
				String UNKNOWN = "Unknown";
				
				String[] elements = (String[]) Attributes.OS_VERSIONS.get(attributes, dataTool);
				if (elements.length == 0) {
					DailyStatistics.OS_VERSIONS.add(UNKNOWN, aggregators);
				} else {
					for (String version : elements) {
						DailyStatistics.OS_VERSIONS.add(version, aggregators);
					}
				}

				elements = (String[]) Attributes.APP_VERSIONS.get(attributes, dataTool);
				if (elements.length == 0) {
					DailyStatistics.APP_VERSIONS.add(UNKNOWN, aggregators);
				} else {
					for (String version : elements) {
						DailyStatistics.APP_VERSIONS.add(version, aggregators);
					}
				}

				elements = (String[]) Attributes.DEVICE_MODELS.get(attributes, dataTool);
				if (elements.length == 0) {
					DailyStatistics.DEVICE_MODELS.add(UNKNOWN, aggregators);
				} else {
					for (String models : elements) {
						DailyStatistics.DEVICE_MODELS.add(models, aggregators);
					}
				}

				elements = (String[]) Attributes.COUNTRIES.get(attributes, dataTool);
				if (elements.length == 0) {
					DailyStatistics.COUNTRIES.add(UNKNOWN, aggregators);
				} else {
					for (String country : elements) {
						DailyStatistics.COUNTRIES.add(country, aggregators);
					}
				}

				elements = (String[]) Attributes.BIRTH_YEARS.get(attributes, dataTool);
				if (elements.length == 0) {
					DailyStatistics.AGES.add(UNKNOWN, aggregators);
				} else {
					for (String birthYear : elements) {
						int age = getAge(birthYear);
						
						if (age > 0) {
							DailyStatistics.AGES.add(String.valueOf(age), aggregators);
						}
					}
				}

				elements = (String[]) Attributes.GENDERS.get(attributes, dataTool);
				if (elements.length == 0) {
					DailyStatistics.GENDERS.add(UNKNOWN, aggregators);
				} else {
					for (String gender : elements) {
						DailyStatistics.GENDERS.add(gender, aggregators);
					}
				}
				

				String from30 = dataTool.beforeDate(29);
				String from7 = dataTool.beforeDate(6);
				
				int freq = timeline.getCount(new Session(from30), new Session(dataTool.baseDate()));
				DailyStatistics.FREQUENCY_USERS30.add(freq, aggregators);
				
				if (purchases.size() > 0) {
					DailyStatistics.FREQUENCY_REVENUE30.add(new Pair<Integer, List<Event>>(freq, purchases), aggregators);
				}

				freq = timeline.getCount(new Session(from7), new Session(dataTool.baseDate()));
				DailyStatistics.FREQUENCY_USERS7.add(freq, aggregators);
				if (purchases.size() > 0) {
					DailyStatistics.FREQUENCY_REVENUE7.add(new Pair<Integer, List<Event>>(freq, purchases), aggregators);
				}
				
				String d0 = timeline.getDate0();
				int r = dataTool.diffDays(dataTool.baseDate(), d0);
				
				DailyStatistics.RETENTION90.add(r, aggregators);
				DailyStatistics.REVENUE90.add(new Pair<Integer, List<Event>>(r, purchases), aggregators);
			}
			
			analytics.set(LineDataTool.asLine(DailyStatistics.asLines(aggregators, dataTool)));
			context.write(key, analytics);
		}
		
		private int getAge(String birthYear) {
			if (birthYear.length() == 8) {
				birthYear = birthYear.substring(0, 4);
			}
			
			if (birthYear.length() == 4) {
				try {
					return Integer.parseInt(dataTool.baseDate().substring(0, 4)) - Integer.parseInt(birthYear) + 1;
				} catch(Exception e) { }
			}
			
			return -1;
		}
	}
}
