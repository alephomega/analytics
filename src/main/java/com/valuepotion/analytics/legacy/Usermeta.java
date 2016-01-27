package com.valuepotion.analytics.legacy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.HCatRecord;
import org.apache.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hcatalog.data.schema.HCatSchema;

import com.valuepotion.analytics.Attributes;
import com.valuepotion.analytics.core.CombineKeyValueTextInputFormat;
import com.valuepotion.analytics.core.HiveAnalyticsReducer;
import com.valuepotion.analytics.core.HiveOutputAnalyticsDriver;
import com.valuepotion.analytics.core.LineDataTool;

public class Usermeta extends HiveOutputAnalyticsDriver {
	
	private static final String[] FIELD_NAMES = new String[] { 

		"basedate", "clientid", "deviceid", "datestr", "dt", "propertyname", "propertyvalue" 
	};
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Usermeta(), args);
	}
	

	@Override
	public Job prepareJob(Configuration conf, String input, String database, String table) throws IOException {
	       
		List<HCatFieldSchema> columns = new ArrayList(FIELD_NAMES.length);
		for (String field : FIELD_NAMES) {
			columns.add(new HCatFieldSchema(field, HCatFieldSchema.Type.STRING, StringUtils.EMPTY));
		}
		
		HCatSchema schema = new HCatSchema(columns);

		Job job = prepareJob(
				conf, 
				"valuepotion.analytics.usermeta", 
				input,
				database, 
				table, 
				CombineKeyValueTextInputFormat.class, 
				Mapper.class,
				Text.class, 
				Text.class, 
				UsermetaReducer.class,
				schema);

		return job;
	}
	

	static class UsermetaReducer extends HiveAnalyticsReducer<Text, Text, WritableComparable, HCatRecord> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] keys = LineDataTool.asFields(key.toString());
		
			for (Text value : values) {
				String[] attributes = Attributes.asFields(value.toString());
				
				String[] firstSession = new String[] { 
						(String) Attributes.FIRST_SESSION_DATE.get(attributes, dataTool),
						(String) Attributes.FIRST_SESSION_TIME.get(attributes, dataTool)
				};
				
				HCatRecord record = new DefaultHCatRecord(FIELD_NAMES.length);

				record.set(0, dataTool.baseDate());
				record.set(1, keys[0]);
				record.set(2, keys[1]);
				record.set(3, firstSession[0]);
				record.set(4, firstSession[1]);
				record.set(5, "first-use");
				record.set(6, null);
				
				context.write(null, record);
				
				String[] installationDates = (String[]) Attributes.INSTALLATION_DATES.get(attributes, dataTool);
				String[] installationTimes = (String[]) Attributes.INSTALLATION_TIMES.get(attributes, dataTool);
				String[] channels = (String[]) Attributes.ATTRIBUTIONS.get(attributes, dataTool);
				
				for (int i = 0; i < installationDates.length; i++) {
					record = new DefaultHCatRecord(FIELD_NAMES.length);
					
					record.set(0, dataTool.baseDate());
					record.set(1, keys[0]);
					record.set(2, keys[1]);
					record.set(3, installationDates[i]);
					record.set(4, installationTimes[i]);
					record.set(5, "install");
					record.set(6, channels[i].length() == 0 || channels[i].equals("0") ? null : channels[i]);
					
					context.write(null, record);
				}
			}
		}
	}
}