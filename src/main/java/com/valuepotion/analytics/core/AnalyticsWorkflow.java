package com.valuepotion.analytics.core;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.cli2.Option;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;


public abstract class AnalyticsWorkflow extends AnalyticsDriver {

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
		String intermediate = optionTool.getOption("intermediate");
		
		String name = workflowName();
		try {
			JobControl control = new JobControl(name);
			control.addJobCollection(controlledJobs(conf, input, output));
			
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

	public abstract Collection<ControlledJob> controlledJobs(Configuration conf, String input, String output) throws IOException;
	public abstract String workflowName();

	@Override
	public Job prepareJob(Configuration conf, String input, String output)
			throws IOException {
		return null;
	}
	
	@Override
	protected void addOptions() {
		super.addOptions();
		
		optionTool.addOption(intermediateOption());
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
