package com.valuepotion.analytics;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.valuepotion.analytics.core.AnalyticsDriver;
import com.valuepotion.analytics.core.AnalyticsMapper;
import com.valuepotion.analytics.core.AnalyticsReducer;
import com.valuepotion.analytics.core.CombineKeyValueTextInputFormat;
import com.valuepotion.analytics.core.LineDataTool;

public class AttributionJoinAnalytics extends AnalyticsDriver {
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new AttributionJoinAnalytics(), args);
	}
	
	@Override
	public Job prepareJob(Configuration conf, String input, String output)
			throws IOException {
		
		Job job = prepareJob(
				conf,
				"valuepotion.analytics.marketing-attribution",
				input,
				output,
				CombineKeyValueTextInputFormat.class, 
				AttributionJoinMapper.class, 
				Text.class, 
				Text.class, 
				AttributionJoinReducer.class, 
				Text.class, 
				Text.class,
				null, 
				AttributionJoinPartitioner.class, 
				AttributionJoinGroupingComparator.class,
				AttributionJoinKeyComparator.class,
				TextOutputFormat.class);
		
		return job;
	}

	static class AttributionJoinMapper extends AnalyticsMapper<Text, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();
		
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			
			Type type = Type.fromSymbol(line);
			if (type == Type.DAILY_SUMMARY) {
				k.set(Type.DAILY_SUMMARY.getSymbol() + key.toString());
				context.write(k, value);
			} else {
				JSONObject attr = (JSONObject) JSONValue.parse(key.toString());
				if (attr == null) {
					return;
				}
				
				try {
					k.set(Type.ATTRIBUTIONS.getSymbol() + LineDataTool.asLine(new String[] {
							(String) attr.get("clientId"), 
							(String) attr.get("deviceId") }));
					
					v.set(LineDataTool.asLine(Type.ATTRIBUTIONS.getSymbol(), 
							new String[] {
								(String) attr.get("trkEventId"), 
								(String) attr.get("pubId") }));
					
					context.write(k, v);
				} catch (Exception e) { }
			}
		}
	}
	
	static class AttributionJoinPartitioner extends Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			int hash = key.toString().substring(1).hashCode();
	        return Math.abs(hash % numPartitions);
		}
	}
	
	static class AttributionJoinGroupingComparator extends WritableComparator {
		
		protected AttributionJoinGroupingComparator() {
			super(Text.class, true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2) {
			Text k1 = (Text) w1;
			Text k2 = (Text) w2;

			return k1.toString().substring(1).compareTo(k2.toString().substring(1));
	    }
	}
	
	static class AttributionJoinKeyComparator extends WritableComparator {
		protected AttributionJoinKeyComparator() {
			super(Text.class, true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2) {
			Text k1 = (Text) w1;
			Text k2 = (Text) w2;

			String s1 = k1.toString();
			String s2 = k2.toString();
			int result = s1.substring(1).compareTo(s2.substring(1));
			if (0 == result) {
				result = Type.DAILY_SUMMARY.getSymbol().equals(String.valueOf(s1.charAt(0))) ? -1 : 1;
			}
			
			return result;
		}
	}

	static class AttributionJoinReducer extends AnalyticsReducer<Text, Text, Text, Text> {
		private Text k = new Text();
		private Text v = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] dailySummary = null;
			String[] installationIDs = null;
			List<String> channelIDs = new LinkedList<String>();;
			
			for (Text value : values) {
				String line = value.toString();

				switch(Type.fromSymbol(line)) {
				case DAILY_SUMMARY: 

					dailySummary = DailySummary.asFields(line);
					
					installationIDs = (String[]) DailySummary.INSTALLATION_IDS.get(dailySummary, dataTool);
					for (int i = 0; i < installationIDs.length; i++) {
						channelIDs.add(Integer.toString(0));
					}
						
					break;
					
				case ATTRIBUTIONS:
					if (installationIDs == null) {
						return;
					}
					
					/*
					 * fields: { installation-id, publisher-id }
					 */
					String[] fields = LineDataTool.asFields(Type.ATTRIBUTIONS.getSymbol(), line);
					int m = find(installationIDs, fields[0]);
					if (m != -1) {
						try {
							channelIDs.set(m, fields[1]);
						} catch (NumberFormatException e) { }
					}
					
					break;
					
				case ATTRIBUTES:
				case ILLEGAL:
					throw new RuntimeException(String.format("Value must start with '~' or '@': (%s, %s)", key.toString(), line));
				}
			}
			
			if (dailySummary != null) {
				k.set(key.toString().substring(1));

				DailySummary.ATTRIBUTIONS.set(dailySummary, channelIDs.toArray(new String[0]), dataTool);
				v.set(DailySummary.asLine(dailySummary));
				
				context.write(k, v);
			}
		}

		private static int find(String[] arr, String value) {
			if (value != null) {
				for (int i = 0; i < arr.length; i++) {
					if (value.equals(arr[i])) {
						return i;
					}
				}
			}
			
			return -1;
		}
	}
}
