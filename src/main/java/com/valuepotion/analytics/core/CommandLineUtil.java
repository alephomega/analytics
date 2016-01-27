package com.valuepotion.analytics.core;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.util.HelpFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.base.Charsets;

public final class CommandLineUtil {

	private CommandLineUtil() { }

	public static void printHelp(Group group) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setGroup(group);
		formatter.print();
	}

	public static void printHelpWithGenericOptions(Group group) throws IOException {
		new GenericOptionsParser(new Configuration(), new org.apache.commons.cli.Options(), new String[0]);
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(System.out, Charsets.UTF_8), true);
		
		HelpFormatter formatter = new HelpFormatter();
		formatter.setGroup(group);
		formatter.setPrintWriter(pw);
		formatter.setFooter("Specify HDFS directories while running on hadoop; else specify local file system directories");
		formatter.print();
	}

	public static void printHelpWithGenericOptions(Group group, OptionException oe) throws IOException {
		new GenericOptionsParser(new Configuration(), new org.apache.commons.cli.Options(), new String[0]);
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(System.out, Charsets.UTF_8), true);
		
		HelpFormatter formatter = new HelpFormatter();
		formatter.setGroup(group);
		formatter.setPrintWriter(pw);
		formatter.setException(oe);
		formatter.print();
	}
}
