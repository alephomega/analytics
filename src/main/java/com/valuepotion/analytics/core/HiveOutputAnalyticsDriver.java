package com.valuepotion.analytics.core;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli2.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hcatalog.data.DefaultHCatRecord;
import org.apache.hcatalog.data.schema.HCatSchema;
import org.apache.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hcatalog.mapreduce.OutputJobInfo;

import com.valuepotion.analytics.DefaultOptionCreator;


public abstract class HiveOutputAnalyticsDriver extends Configured implements Tool {
	
	protected CommandLineOptionTool optionTool = new CommandLineOptionTool();

	@Override
	public int run(String[] args) throws Exception {
		addOptions();
		
		Map<String, String> argumentMap = optionTool.parseArguments(args);
		if (argumentMap == null) {
			return -1;
		}
		
		Configuration conf = setAttributes(getConf(), argumentMap);
		String input = optionTool.getOption("input");
		String database = optionTool.getOption("database");
		String table = optionTool.getOption("table");
		
		Job job = prepareJob(conf, input, database, table);
		job.waitForCompletion(true);
		
		return 0;
	}
	
	protected void addOptions() {
		optionTool.addOption(DefaultOptionCreator.inputOption().create());
		optionTool.addOption(DefaultOptionCreator.baseDateOption().create());
		
		optionTool.addOption(databaseOption());
		optionTool.addOption(tableOption());
	}
	
	protected Configuration setAttributes(Configuration conf, Map<String, String> argumentMap) {
		for (Iterator iterator = argumentMap.entrySet().iterator(); iterator.hasNext();) {
			Entry<String, String> entry = (Entry<String, String>) iterator.next();
			
			String key = entry.getKey();
			String value = entry.getValue();
			
			if (value != null) {
				conf.set(String.format("valuepotion.analytics.%s", key.substring(2)), value);
			}
		}
		
		return conf;
	}
	
	public abstract Job prepareJob(Configuration conf, String input, String database, String table) throws IOException;

	protected Job prepareJob(
			Configuration conf,
			String name,
			String input,
			String database,
			String table,
			Class<? extends InputFormat> inputFormat,
			Class<? extends Mapper> mapper,
			Class<? extends Writable> mapperKey,
			Class<? extends Writable> mapperValue,
			Class<? extends HiveAnalyticsReducer> reducer,
			HCatSchema schema) throws IOException {
		
		Job job = Job.getInstance(conf);
		job.setJobName(name);
		
		Configuration jobConf = job.getConfiguration();
		
		if (reducer.equals(Reducer.class)) {
			if (mapper.equals(Mapper.class)) {
				throw new IllegalStateException("Can't figure out the user class jar file from mapper/reducer");
			}
			job.setJarByClass(mapper);
		} else {
			job.setJarByClass(reducer);
		}

		job.setInputFormatClass(inputFormat);
		
		if (input != null) {
			if (FileInputFormat.class.isAssignableFrom(inputFormat)) {
				FileInputFormat.setInputPaths(job, input);
			} else {
				jobConf.set("mapred.input.dir", input);
			}
		}

		job.setMapperClass(mapper);
		job.setMapOutputKeyClass(mapperKey);
		job.setMapOutputValueClass(mapperValue);
		jobConf.setBoolean("mapred.compress.map.output", true);

		job.setReducerClass(reducer);
		job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(DefaultHCatRecord.class);
        
		OutputJobInfo jobInfo = OutputJobInfo.create(database, table, null);
		jobInfo.setOutputSchema(schema);
		
		HCatOutputFormat.setOutput(job, jobInfo);
        HCatOutputFormat.setSchema(job, schema);
        job.setOutputFormatClass(HCatOutputFormat.class);
        
		return job;
	}
	

	private static Option databaseOption() {
		return CommandLineOptionTool.buildOption("database", "db", "database name, which if null 'default' is used", true, false, "default");
	}

	private static Option tableOption() {
		return CommandLineOptionTool.buildOption("table", "t", "table name", true, true, null);
	}
}