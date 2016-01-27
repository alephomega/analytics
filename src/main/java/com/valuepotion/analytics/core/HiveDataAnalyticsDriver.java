package com.valuepotion.analytics.core;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli2.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hcatalog.mapreduce.HCatInputFormat;

import com.valuepotion.analytics.DefaultOptionCreator;


public abstract class HiveDataAnalyticsDriver extends Configured implements Tool {
	
	protected CommandLineOptionTool optionTool = new CommandLineOptionTool();

	@Override
	public int run(String[] args) throws Exception {
		addOptions();
		
		Map<String, String> argumentMap = optionTool.parseArguments(args);
		if (argumentMap == null) {
			return -1;
		}
		
		Configuration conf = setAttributes(getConf(), argumentMap);
		String database = optionTool.getOption("database");
		String table = optionTool.getOption("table");
		String filter = optionTool.getOption("filter");
		String output = optionTool.getOption("output");
		
		Job job = prepareJob(conf, database, table, filter, output);
		job.waitForCompletion(true);
		
		return 0;
	}
	
	protected void addOptions() {
		optionTool.addOption(DefaultOptionCreator.outputOption().create());
		optionTool.addOption(DefaultOptionCreator.overwriteOption().create());
		optionTool.addOption(DefaultOptionCreator.baseDateOption().create());
		
		optionTool.addOption(databaseOption());
		optionTool.addOption(tableOption());
		optionTool.addOption(filterOption());
		optionTool.addOption(columnsOption());
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
	
	public abstract Job prepareJob(Configuration conf, String database, String table, String filter, String output) throws IOException;

	protected Job prepareJob(
			Configuration conf,
			String name,
			String database,
			String table,
			String filter,
			String output,
			Class<? extends HiveAnalyticsMapper> mapper,
			Class<? extends Writable> mapperKey,
			Class<? extends Writable> mapperValue,
			Class<? extends Reducer> reducer,
			Class<? extends Writable> reducerKey,
			Class<? extends Writable> reducerValue,
			Class<? extends Reducer> combiner,
			Class<? extends Partitioner> partitioner,
			Class<? extends WritableComparator> groupingComparator,
			Class<? extends WritableComparator> comparator,
			Class<? extends OutputFormat> outputFormat) throws IOException {
		
		Job job = Job.getInstance(conf);
		job.setJobName(name);
		
		Configuration jobConf = job.getConfiguration();
		
		if (filter != null) {
			HCatInputFormat.setInput(jobConf, database, table, filter);
		} else {
			HCatInputFormat.setInput(jobConf, database, table);
		}
		
		if (reducer.equals(Reducer.class)) {
			if (mapper.equals(Mapper.class)) {
				throw new IllegalStateException("Can't figure out the user class jar file from mapper/reducer");
			}
			job.setJarByClass(mapper);
		} else {
			job.setJarByClass(reducer);
		}

		job.setInputFormatClass(HCatInputFormat.class);

		job.setMapperClass(mapper);
		job.setMapOutputKeyClass(mapperKey);
		job.setMapOutputValueClass(mapperValue);
		jobConf.setBoolean("mapred.compress.map.output", true);

		job.setReducerClass(reducer);
		job.setOutputKeyClass(reducerKey);
		job.setOutputValueClass(reducerValue);

		if (combiner != null) {
			job.setCombinerClass(combiner);
		}

		if (partitioner != null) {
			job.setPartitionerClass(partitioner);
		}
		
		if (groupingComparator != null) {
			job.setGroupingComparatorClass(groupingComparator);
			job.setCombinerKeyGroupingComparatorClass(groupingComparator);
		}

		if (comparator != null) {
			job.setSortComparatorClass(comparator);
		}

		job.setOutputFormatClass(outputFormat);

		if (output != null) {
			if (FileOutputFormat.class.isAssignableFrom(outputFormat)) {
				FileOutputFormat.setOutputPath(job, new Path(output));
			} else {
				jobConf.set("mapred.output.dir", output);
			}
		}

		if (optionTool.hasOption("overwrite")) {
			HdfsTool.delete(jobConf, new Path(output));
		}

		return job;
	}
	

	private static Option databaseOption() {
		return CommandLineOptionTool.buildOption("database", "db", "database name, which if null 'default' is used", true, false, "default");
	}

	private static Option tableOption() {
		return CommandLineOptionTool.buildOption("table", "t", "table name", true, true, null);
	}

	private static Option filterOption() {
		return CommandLineOptionTool.buildOption("filter", "f", "partition filter clause", true, false, null);
	}

	private static Option columnsOption() {
		return CommandLineOptionTool.buildOption("columns", "c", "column names", true, true, null);
	}
}