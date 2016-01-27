package com.valuepotion.analytics.core;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.valuepotion.analytics.DefaultOptionCreator;

public abstract class AnalyticsDriver extends Configured implements Tool {
	
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
		String output = optionTool.getOption("output");
		
		Job job = prepareJob(conf, input, output);
		job.waitForCompletion(true);
		
		return 0;
	}

	protected void addOptions() {
		optionTool.addOption(DefaultOptionCreator.inputOption().create());
		optionTool.addOption(DefaultOptionCreator.outputOption().create());
		optionTool.addOption(DefaultOptionCreator.overwriteOption().create());
		optionTool.addOption(DefaultOptionCreator.baseDateOption().create());
		optionTool.addOption(DefaultOptionCreator.fieldSeparatorOption().create());
	}

	public abstract Job prepareJob(Configuration conf, String input, String output) throws IOException;

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
	
	protected Job prepareJob(
			Configuration conf,
			String name,
			String input,
			String output,
			Class<? extends InputFormat> inputFormat,
			Class<? extends Mapper> mapper,
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

		if (!mapper.equals(Mapper.class)) {
			job.setJarByClass(mapper);
		} else if (!reducer.equals(Reducer.class)) {
			job.setJarByClass(reducer);
		} else {
			job.setJarByClass(this.getClass());
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
}
