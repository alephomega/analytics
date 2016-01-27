package com.valuepotion.analytics.core;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.cli2.Option;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public abstract class HiveDataAnalyticsWorkflow extends HiveDataAnalyticsDriver {

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
		String intermediate = optionTool.getOption("intermediate");
		
		String name = workflowName();
		try {
			JobControl control = new JobControl(name);
			control.addJobCollection(controlledJobs(conf, database, table, filter, output));
			
			Thread workflow = new Thread(control, String.format("%s-thread", name));
			workflow.setDaemon(true);
			workflow.start();
			
			while (!control.allFinished()){
				Thread.sleep(500);
			}
			
			if (control.getFailedJobList().size() > 0) {
				System.err.println(control.getFailedJobList().size() + " jobs failed!");
				for (ControlledJob job : control.getFailedJobList()) {
					System.err.println(job.getJobName() + " failed");
				}
			} else {
				System.out.println("workflow completed [" + control.getSuccessfulJobList().size() + "] jobs");
			}
			
		} finally {
			HdfsTool.delete(conf, new Path(intermediate));
		}
		
		return 0;
	}

	public abstract Collection<ControlledJob> controlledJobs(Configuration conf, String database, String table, String filter, String output) throws IOException;
	public abstract String workflowName();

	
	@Override
	protected void addOptions() {
		super.addOptions();
		
		optionTool.addOption(intermediateOption());
	}
	
	@Override
	public Job prepareJob(Configuration conf, String database, String table, String filter, String output) throws IOException {
		return null;
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

	public String intermediatePath(String name) {
		String intermediate = optionTool.getOption("intermediate");
		Path path = new Path(new Path(intermediate), name);
		
		return path.toString();
	}
	
	private static Option intermediateOption() {
		return CommandLineOptionTool.buildOption(
				"intermediate",
				"t", 
				"Path to job intermediate directory.", 
				true, 
				true, 
				null);
	}
}
