package com.valuepotion.analytics;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.data.HCatRecord;

import com.valuepotion.analytics.core.AnalyticsReducer;
import com.valuepotion.analytics.core.HiveAnalyticsMapper;
import com.valuepotion.analytics.core.HiveDataAnalyticsWorkflow;
import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.core.LineDataTool.FieldSeparator;

public class DailySummaryAnalytics extends HiveDataAnalyticsWorkflow {
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DailySummaryAnalytics(), args);
	}
	

	@Override
	public Collection<ControlledJob> controlledJobs(Configuration conf, String database, String table, String filter, String output) throws IOException {
		String intermediate = intermediatePath("temporary");
		
		ControlledJob step1 = new ControlledJob(step1(conf, database, table, filter, intermediate), null);
		ControlledJob step2 = new ControlledJob(step2(conf, intermediate, output), Arrays.asList(step1));

		return Arrays.asList(step1, step2);
	}

	@Override
	public String workflowName() {
		return "daily-summary-analytics";
	}
	
	
	private Job step1(Configuration conf, String database, String table, String filter, String output)
			throws IOException {
		
		return prepareJob(
				conf,
				"valuepotion.analytics.daily-summary.step1",
				database,
				table,
				filter,
				output,
				Step1Mapper.class, 
				Text.class, 
				Text.class, 
				Step1Reducer.class, 
				Text.class, 
				Text.class,
				null, 
				null,
				null,
				null,
				SequenceFileOutputFormat.class);
	}
	
	private Job step2(Configuration conf, String input, String output)
			throws IOException {
		
		return prepareJob(
				conf,
				"valuepotion.analytics.daily-summary.step2",
				input,
				output,
				SequenceFileInputFormat.class, 
				Mapper.class, 
				Text.class, 
				Text.class, 
				Step2Reducer.class, 
				Text.class, 
				Text.class,
				null, 
				Step2Partitioner.class,
				Step2GroupingComparator.class,
				Step2KeyComparator.class,
				TextOutputFormat.class);
	}
	

	
	static class Step1Mapper extends HiveAnalyticsMapper<LongWritable, HCatRecord, Text, Text> {
		/*
		private CharsetEncoder encoder;
		private CharsetDecoder decoder;
		private CharBuffer buffer;
		*/
		
		private Text k = new Text();
		private Text v = new Text();
		
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			
			/*
			encoder = StandardCharsets.UTF_8.newEncoder();
			encoder.onMalformedInput(CodingErrorAction.REPLACE);
			encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
			
			decoder = StandardCharsets.UTF_8.newDecoder();
			decoder.onMalformedInput(CodingErrorAction.REPLACE);
			decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
			
			buffer = CharBuffer.allocate(1024);
			*/
		}
		
		@Override
		protected void map(LongWritable key, HCatRecord value, Context context) throws IOException, InterruptedException {
			
			/*
			 * fields: { 
			 * 	0: P_CLIENTID 			1: DEVICEID 			2: SESSION 				3: REVENUEMOUNT 	
			 * 	4: CURRENCY 			5: EVENTNAME 			6: EVENTID				7: DT
			 * 	8: DEVICEOS				9: DEVICEOSVERSION		10: APPVERSION 			11: DEVICEMODELNAME 	12: COUNTRY 
			 * 	13: USERINFO_BIRTH	 	14: USERINFO_GENDER 	15: USERINFO_LEVEL
			 * }
			 * 
			 */
			
			String[] fields = getFields(value);
			
			String ts = ts(fields[7]);
			k.set(LineDataTool.asLine(new String[] { fields[0], fields[1], fields[2] }));
			
			double r = 0d;
			try {
				r = Double.parseDouble(fields[3]);
			} catch (NumberFormatException e) { }

			v.set(LineDataTool.asLine(
					new String[] { 
						fields[5].equals("install") ? fields[6] : StringUtils.EMPTY,
						fields[5].equals("update") ? Integer.toString(1) : Integer.toString(0),
						r > 0 ? LineDataTool.asLine(FieldSeparator.ELEMENTS, new String[] { fields[4], String.valueOf(r) }) : StringUtils.EMPTY,
						ts,
						validate(fields[8]) + validate(fields[9]), 
						validate(fields[10]), 
						validate(fields[11]),
						validate(fields[12]), 
						validate(fields[13]),
						validate(fields[14]), 
						validate(fields[15])
					}));
			
			context.write(k, v);
		}

		private String validate(String s) throws CharacterCodingException {
			return s.length() > 0 ? s.replaceAll("[\r\n]", StringUtils.EMPTY) : s;
			
			/*
			if (s.length() == 0) {
				return s;
			}
			
			buffer.clear();
			buffer.put(s);
			buffer.flip();
			
			return decoder.decode(encoder.encode(buffer)).toString().replaceAll("[\r\n]", StringUtils.EMPTY);
			*/
		}

		private String ts(String time) {
			return time;
		}
	}
	
	static class Step1Reducer extends AnalyticsReducer<Text, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();
		
		private DateFormat dateFormatter = new SimpleDateFormat("yyyyMMdd HHmmss");
		
		
		private static class Agglomerator {
			private List<String> elements = new ArrayList<String>();
			
			void add(String line) {
				elements.add(LineDataTool.elements2Children(line));
			}
			
			String[] elements() {
				return elements.toArray(new String[0]);
			}
		}

		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] sessionTime = new String[] { "99999999 999999", "00000000 000000" };
			
			Attribution attribution = null;
			Agglomerator agglomerator = new Agglomerator();
			int updateCount = 0;

			UniqueFilter filter = new UniqueFilter();
			
			String[] fields = null;
			for (Text value : values) {
				fields = LineDataTool.asFields(value.toString());
				
				String ts = fields[3];
				if (!LineDataTool.isNA(fields[0])) {
					if (attribution == null || attribution.getTime().compareTo(ts) > 0) {
						attribution = new Attribution(fields[0], ts);
					}
				}
				
				updateCount += Integer.parseInt(fields[1]);
				if (!LineDataTool.isNA(fields[2])) {
					agglomerator.add(fields[2]);
				}
				
				if (sessionTime[0].compareTo(ts) > 0) {
					sessionTime[0] = ts;
				}

				if (sessionTime[1].compareTo(ts) < 0) {
					sessionTime[1] = ts;
				}
				
				filter.add(SessionData.LEVELS.getIndex(), fields[10]);
			}
			
			int sd = 0;
			try {
				sd = (int) (dateFormatter.parse(sessionTime[1]).getTime() - dateFormatter.parse(sessionTime[0]).getTime()) / 1000;
			} catch (ParseException e) { }
			
			String[] keys = LineDataTool.asFields(key.toString());
			k.set(LineDataTool.asLine(new String[] { sessionTime[0] + keys[0], keys[1] }));
			
			String[] session = SessionData.init();
			SessionData.INSTALLATION_ID.set(
					session, 
					attribution == null ? StringUtils.EMPTY : attribution.getId(), 
					dataTool);
			
			SessionData.INSTALLATION_TIME.set(
					session, 
					attribution == null ? StringUtils.EMPTY : attribution.getTime(), 
					dataTool);

			SessionData.UPDATE_COUNT.set(
					session, 
					updateCount,
					dataTool);
			
			SessionData.PURCHASES.set(
					session, 
					agglomerator.elements(), 
					dataTool);

			SessionData.SESSION_START_TIME.set(
					session, 
					sessionTime[0], 
					dataTool);

			SessionData.SESSION_DURATION.set(
					session, 
					sd, 
					dataTool);

			SessionData.OS_VERSION.set(
					session, 
					fields[4], 
					dataTool);
	
			SessionData.APP_VERSION.set(
					session, 
					fields[5], 
					dataTool);
			
			SessionData.DEVICE_MODEL.set(
					session, 
					fields[6], 
					dataTool);
			
			SessionData.COUNTRY.set(
					session, 
					fields[7], 
					dataTool);
			
			SessionData.BIRTH_YEAR.set(
					session, 
					fields[8], 
					dataTool);
			
			SessionData.GENDER.set(
					session, 
					fields[9], 
					dataTool);

			SessionData.LEVELS.set(
					session, 
					filter.values(SessionData.LEVELS.getIndex()), 
					dataTool);
			
			v.set(SessionData.asLine(session));
			
			context.write(k, v);
		}
	}
	
	
	private static final int SECONDARY_SORTING_KEYSIZE = "yyyyMMdd HHmmss".length();

	static class Step2Partitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
	        return Math.abs(key.toString().substring(SECONDARY_SORTING_KEYSIZE).hashCode() % numPartitions);
		}
	}
	
	static class Step2GroupingComparator extends WritableComparator {
		
		protected Step2GroupingComparator() {
			super(Text.class, true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2) {
			return ((Text) w1).toString().substring(SECONDARY_SORTING_KEYSIZE).compareTo(((Text) w2).toString().substring(SECONDARY_SORTING_KEYSIZE));
	    }
	}
	
	static class Step2KeyComparator extends WritableComparator {
		protected Step2KeyComparator() {
			super(Text.class, true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2) {
			String s1 = ((Text) w1).toString();
			String s2 = ((Text) w2).toString();

			int result = s1.substring(SECONDARY_SORTING_KEYSIZE).compareTo(s2.substring(SECONDARY_SORTING_KEYSIZE));
			if (0 == result) {
				result = s1.substring(0, SECONDARY_SORTING_KEYSIZE).compareTo(s2.substring(0, SECONDARY_SORTING_KEYSIZE));
			}
			
			return result;
		}
	}
	
	
	static class Step2Reducer extends AnalyticsReducer<Text, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			List<Attribution> attributions = new LinkedList<Attribution>();
			
			SessionLineBuilder sessionsBuilder = new SessionLineBuilder(dataTool);
			PurchaseLineBuilder purchasesBuilder = new PurchaseLineBuilder(dataTool);
			UpdateLineBuilder updatesBuilder = new UpdateLineBuilder(dataTool);
			
			UniqueFilter filter = new UniqueFilter();
			
			String firstSession = null;
			
			for (Text value : values) {
				String[] sessionRecord = SessionData.asFields(value.toString());
				
				if (SessionData.isFirstSession(sessionRecord, dataTool)) {
					
					attributions.add(new Attribution(
							(String) SessionData.INSTALLATION_ID.get(sessionRecord, dataTool),
							(String) SessionData.INSTALLATION_TIME.get(sessionRecord, dataTool)));
					
					updatesBuilder.onAttributionChanged();
					sessionsBuilder.onAttributionChanged();
					purchasesBuilder.onAttributionChanged();
				}

				filter.add(
						SessionData.OS_VERSION.getIndex(), 
						(String) SessionData.OS_VERSION.get(sessionRecord, dataTool));
				
				filter.add(
						SessionData.APP_VERSION.getIndex(), 
						(String) SessionData.APP_VERSION.get(sessionRecord, dataTool));
				
				filter.add(
						SessionData.DEVICE_MODEL.getIndex(), 
						(String) SessionData.DEVICE_MODEL.get(sessionRecord, dataTool));
				
				filter.add(
						SessionData.COUNTRY.getIndex(), 
						(String) SessionData.COUNTRY.get(sessionRecord, dataTool));
				
				filter.add(
						SessionData.BIRTH_YEAR.getIndex(), 
						(String) SessionData.BIRTH_YEAR.get(sessionRecord, dataTool));
				
				filter.add(
						SessionData.GENDER.getIndex(), 
						(String) SessionData.GENDER.get(sessionRecord, dataTool));
				
				filter.add(
						SessionData.LEVELS.getIndex(), 
						(String[]) SessionData.LEVELS.get(sessionRecord, dataTool));
				
				if (firstSession == null) {
					firstSession = (String) SessionData.SESSION_START_TIME.get(sessionRecord, dataTool);
				}

				updatesBuilder.add(
						SessionData.UPDATE_COUNT.getString(sessionRecord, dataTool));
				
				purchasesBuilder.add(
						SessionData.PURCHASES.getString(sessionRecord, dataTool));
				
				sessionsBuilder.add(
						SessionData.SESSION_DURATION.getString(sessionRecord, dataTool));
			}
			
			String[] keys = LineDataTool.asFields(key.toString().substring(SECONDARY_SORTING_KEYSIZE));
			k.set(LineDataTool.asLine(new String[] { keys[0], keys[1] }));

			String[] dailySummary = DailySummary.init();
			
			DailySummary.INSTALLATION_COUNT.set(
					dailySummary, 
					attributions.size(),
					dataTool);
			
			SessionSummary total = sessionsBuilder.getTotal();
			DailySummary.SESSION_COUNT.set(
					dailySummary, 
					total.getCount(),
					dataTool);
			
			DailySummary.SESSION_DURATION.set(
					dailySummary, 
					total.getDuration(),
					dataTool);
			
			DailySummary.FIRST_SESSION_TIME.set(
					dailySummary, 
					firstSession,
					dataTool);
			
			Collections.sort(attributions);
			String[] installationIDs = new String[attributions.size()];
			for (int i = 0; i < attributions.size(); i++) {
				installationIDs[i] = attributions.get(i).getId();
			}

			DailySummary.INSTALLATION_IDS.set(
					dailySummary, 
					installationIDs,
					dataTool);

			String[] installationTimes = new String[attributions.size()];
			for (int i = 0; i < attributions.size(); i++) {
				installationTimes[i] = attributions.get(i).getTime();
			}
			
			DailySummary.INSTALLATION_TIMES.set(
					dailySummary, 
					installationTimes,
					dataTool);
			
			
			DailySummary.UPDATES.set(
					dailySummary, 
					updatesBuilder.get(),
					dataTool);
			
			DailySummary.SESSIONS.set(
					dailySummary, 
					sessionsBuilder.get(),
					dataTool);

			DailySummary.PURCHASES.set(
					dailySummary, 
					purchasesBuilder.get(),
					dataTool);
			
			DailySummary.OS_VERSIONS.set(
					dailySummary, 
					filter.values(SessionData.OS_VERSION.getIndex()),
					dataTool);
			
			DailySummary.APP_VERSIONS.set(
					dailySummary, 
					filter.values(SessionData.APP_VERSION.getIndex()),
					dataTool);
			
			DailySummary.DEVICE_MODELS.set(
					dailySummary, 
					filter.values(SessionData.DEVICE_MODEL.getIndex()),
					dataTool);
			
			DailySummary.COUNTRIES.set(
					dailySummary, 
					filter.values(SessionData.COUNTRY.getIndex()),
					dataTool);
			
			DailySummary.BIRTH_YEARS.set(
					dailySummary, 
					filter.values(SessionData.BIRTH_YEAR.getIndex()),
					dataTool);
			
			DailySummary.GENDERS.set(
					dailySummary, 
					filter.values(SessionData.GENDER.getIndex()),
					dataTool);
			
			DailySummary.LEVELS.set(
					dailySummary, 
					filter.values(SessionData.LEVELS.getIndex()),
					dataTool);

			v.set(DailySummary.asLine(dailySummary));
			
			context.write(k, v);
		}
	}

	
	static class UniqueFilter {
		
		private TreeSet<String> set = new TreeSet<String>();
		
		void add(int field, String... values) {
			for (String value : values) {
				if (!LineDataTool.isNA(value)) {
					for (String element : LineDataTool.asFields(FieldSeparator.ELEMENTS, value)) {
						set.add(LineDataTool.asLine(FieldSeparator.ELEMENTS, new String[] { String.format("%02X", field), element }));
					}
				}
			}
		}
		
		String[] values(int field) {
			Set<String> fieldSet = set.subSet(String.format("%02X", field), String.format("%02X", (field + 1)));
			
			String[] a = fieldSet.toArray(new String[0]);
			for (int i = 0; i < a.length; i++) {
				a[i] = LineDataTool.asFields(FieldSeparator.ELEMENTS, a[i])[1];
			}
			
			return a;
		}
		
		String asLine(int field) {
			return LineDataTool.asLine(FieldSeparator.ELEMENTS, values(field));
		}
	}
}
