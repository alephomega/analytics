package com.valuepotion.analytics.core;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;

import com.valuepotion.analytics.DefaultOptionCreator;

public class CommandLineOptionTool {
	private final List<Option> options = new LinkedList<Option>();
	private Map<String, String> argumentMap;
	

	public void addFlag(String name, String shortName, String description) {
		options.add(buildOption(name, shortName, description, false, false, null));
	}

	public void addOption(String name, String shortName, String description) {
		options.add(buildOption(name, shortName, description, true, false, null));
	}

	public void addOption(String name, String shortName, String description, boolean required) {
		options.add(buildOption(name, shortName, description, true, required, null));
	}

	public void addOption(String name, String shortName, String description, String defaultValue) {
		options.add(buildOption(name, shortName, description, true, false, defaultValue));
	}

	public Option addOption(Option option) {
		options.add(option);
		return option;
	}
	
	public static Option buildOption(
			String name,
			String shortName,
			String description,
			boolean hasArg,
			boolean required,
			String defaultValue) {

		DefaultOptionBuilder optionBuilder = new DefaultOptionBuilder().withLongName(name).withDescription(description).withRequired(required);

		if (shortName != null) {
			optionBuilder.withShortName(shortName);
		}

		if (hasArg) {
			ArgumentBuilder argumentBuilder = new ArgumentBuilder().withName(name).withMinimum(1).withMaximum(1);

			if (defaultValue != null) {
				argumentBuilder = argumentBuilder.withDefault(defaultValue);
			}

			optionBuilder.withArgument(argumentBuilder.create());
		}

		return optionBuilder.create();
	}

	public Map<String, String> parseArguments(String[] args) throws IOException {
		Option helpOpt = addOption(DefaultOptionCreator.helpOption());
		
		GroupBuilder groupBuilder = new GroupBuilder().withName("Job-Specific Options:");

		for (Option opt : options) {
			groupBuilder = groupBuilder.withOption(opt);
		}

		Group group = groupBuilder.create();

		CommandLine cmdLine;
		try {
			Parser parser = new Parser();
			parser.setGroup(group);
			parser.setHelpOption(helpOpt);
			cmdLine = parser.parse(args);

		} catch (OptionException e) {
			CommandLineUtil.printHelpWithGenericOptions(group, e);
			return null;
		}

		if (cmdLine.hasOption(helpOpt)) {
			CommandLineUtil.printHelpWithGenericOptions(group);
			return null;
		}

		argumentMap = new TreeMap<String, String>();
		maybePut(argumentMap, cmdLine, this.options.toArray(new Option[this.options.size()]));

		return argumentMap;
	}
	
	protected static void maybePut(Map<String, String> args, CommandLine cmdLine, Option... opt) {
		for (Option o : opt) {

			if (cmdLine.hasOption(o) || cmdLine.getValue(o) != null) {

				Object vo = cmdLine.getValue(o);
				String value = vo == null ? null : vo.toString();
				args.put(o.getPreferredName(), value);
			}
		}
	}

	public static String keyFor(String optionName) {
		return "--" + optionName;
	}

	public String getOption(String optionName) {
		return argumentMap.get(keyFor(optionName));
	}

	public boolean hasOption(String optionName) {
		return argumentMap.containsKey(keyFor(optionName));
	}
}
