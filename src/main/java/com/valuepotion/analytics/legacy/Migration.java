package com.valuepotion.analytics.legacy;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.data.HCatRecord;

import com.valuepotion.analytics.Attributes;
import com.valuepotion.analytics.PurchaseSummary;
import com.valuepotion.analytics.SessionSummary;
import com.valuepotion.analytics.Type;
import com.valuepotion.analytics.UpdateSummary;
import com.valuepotion.analytics.bases.CustomerEvent;
import com.valuepotion.analytics.core.AnalyticsReducer;
import com.valuepotion.analytics.core.HiveAnalyticsMapper;
import com.valuepotion.analytics.core.HiveDataAnalyticsDriver;
import com.valuepotion.analytics.core.LineDataTool;

public class Migration extends HiveDataAnalyticsDriver {
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Migration(), args);
	}
	
	@Override
	public Job prepareJob(Configuration conf, String database, String table, String filter, String output) throws IOException {
		return prepareJob(
				conf,
				"valuepotion.analytics.migration",
				database,
				table,
				filter,
				output,
				MigrationMapper.class, 
				Text.class, 
				Text.class, 
				MigrationReducer.class, 
				Text.class, 
				Text.class,
				null, 
				MigrationPartitioner.class,
				MigrationGroupingComparator.class,
				MigrationKeyComparator.class,
				TextOutputFormat.class);
	}
	
	static class MigrationMapper extends HiveAnalyticsMapper<LongWritable, HCatRecord, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();
		
		@Override
		protected void map(LongWritable key, HCatRecord value, Context context) throws IOException, InterruptedException {
			/*
			 *  0: clientid, 1: deviceid, 2: datestr, 3: dt, 4: propertyname, 5:propertyvalue
			 */
			
			String[] fields = getFields(value);
			k.set(LineDataTool.asLine(new String[] { fields[0], fields[1], fields[3] }));
			v.set(LineDataTool.asLine(new String[] { 
					fields[2],  
					fields[3],  
					fields[4].equals("install") ? "1" : "0",
					fields[5].equals("\\N") ? StringUtils.EMPTY : fields[5]		
			}));
			
			context.write(k, v);
		}
	}

	static class MigrationPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String s = key.toString();
			String[] fields = LineDataTool.asFields(s);
			
			int hash = LineDataTool.asLine(new String[] { fields[0], fields[1] }).hashCode();
	        return Math.abs(hash % numPartitions);
		}
	}
	
	static class MigrationGroupingComparator extends WritableComparator {
		
		protected MigrationGroupingComparator() {
			super(Text.class, true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2) {
			Text k1 = (Text) w1;
			Text k2 = (Text) w2;
			
			String[] f1 = LineDataTool.asFields(k1.toString());
			String[] f2 = LineDataTool.asFields(k2.toString());

			return LineDataTool.asLine(new String[] { f1[0], f1[1] }).compareTo(LineDataTool.asLine(new String[] { f2[0], f2[1] }));
	    }
	}
	
	static class MigrationKeyComparator extends WritableComparator {
		protected MigrationKeyComparator() {
			super(Text.class, true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2) {
			Text k1 = (Text) w1;
			Text k2 = (Text) w2;

			String[] f1 = LineDataTool.asFields(k1.toString());
			String[] f2 = LineDataTool.asFields(k2.toString());
			
			int result = LineDataTool.asLine(new String[] { f1[0], f1[1] }).compareTo(LineDataTool.asLine(new String[] { f2[0], f2[1] }));
			if (0 == result) {
				result = f1[2].compareTo(f2[2]);
			}
			
			return result;
		}
	}
	
	static class MigrationReducer extends AnalyticsReducer<Text, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();
		
		private List<String> installationDates = new LinkedList<String>();
		private List<String> installationTimes = new LinkedList<String>();
		private List<String> channels = new LinkedList<String>();
		private List<List<CustomerEvent<SessionSummary>>> sessions = new LinkedList<List<CustomerEvent<SessionSummary>>>();
		private List<List<CustomerEvent<PurchaseSummary[]>>> purchases = new LinkedList<List<CustomerEvent<PurchaseSummary[]>>>();
		private List<List<CustomerEvent<UpdateSummary>>> updates = new LinkedList<List<CustomerEvent<UpdateSummary>>>(); 
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] keys = LineDataTool.asFields(key.toString());
			k.set(LineDataTool.asLine(new String[] { keys[0], keys[1] }));
			
			String[] attributes = Attributes.init();
			
			String[] firstSession = null;
			
			for (Text value : values) {
				String[] fields = LineDataTool.asFields(value.toString());
				
				if (firstSession == null) {
					firstSession = new String[] { fields[0], fields[1] };
				}
				
				if (Integer.parseInt(fields[2]) == 1) {
					installationDates.add(fields[0]);
					installationTimes.add(fields[1]);
					
					channels.add(LineDataTool.isNA(fields[3]) ? "0" : fields[3]);
				}
			}
			
			int installationCount = installationDates.size();
			
			Attributes.INSTALLATION_COUNT.set(
					attributes, 
					installationCount, 
					dataTool);
			
			Attributes.FIRST_SESSION_DATE.set(
					attributes, 
					firstSession == null ? StringUtils.EMPTY : firstSession[0], 
					dataTool);
			
			Attributes.FIRST_SESSION_TIME.set(
					attributes, 
					firstSession == null ? StringUtils.EMPTY : firstSession[1], 
					dataTool);
			
			Attributes.INSTALLATION_DATES.set(
					attributes, 
					installationDates.toArray(new String[0]), 
					dataTool);
			
			installationDates.clear();
			
			Attributes.INSTALLATION_TIMES.set(
					attributes, 
					installationTimes.toArray(new String[0]), 
					dataTool);
			
			installationTimes.clear();
			
			
			if (installationCount > 0) {
				for (int i = 0; i <= installationCount; i++) {
					sessions.add(new LinkedList<CustomerEvent<SessionSummary>>());
				}
				
				Attributes.SESSIONS.set(attributes, sessions, dataTool);
				sessions.clear();

				for (int i = 0; i <= installationCount; i++) {
					purchases.add(new LinkedList<CustomerEvent<PurchaseSummary[]>>());
				}
				
				Attributes.PURCHASES.set(attributes, purchases, dataTool);
				purchases.clear();

				for (int i = 0; i <= installationCount; i++) {
					updates.add(new LinkedList<CustomerEvent<UpdateSummary>>());
				}
				
				Attributes.UPDATES.set(attributes, updates, dataTool);
				updates.clear();
			}
			
			
			Attributes.ATTRIBUTIONS.set(
					attributes, 
					channels.toArray(new String[0]), 
					dataTool);
			
			channels.clear();
			
			
			v.set(LineDataTool.asLine(Type.ATTRIBUTES.getSymbol(), attributes));
			
			context.write(k, v);
		}
	}
}