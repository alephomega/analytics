package com.valuepotion.analytics;

import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;

public final class DefaultOptionCreator {

	private DefaultOptionCreator() {

	}

	public static Option helpOption() {
		return new DefaultOptionBuilder().withLongName("help")
				.withDescription("Print out help").withShortName("h").create();
	}

	public static DefaultOptionBuilder inputOption() {
		return new DefaultOptionBuilder()
				.withLongName("input")
				.withRequired(true)
				.withShortName("i")
				.withArgument(
						new ArgumentBuilder().withName("input")
								.withMinimum(1).withMaximum(1).create())
				.withDescription("Path to job input directory.");
	}

	public static DefaultOptionBuilder outputOption() {
		return new DefaultOptionBuilder()
				.withLongName("output")
				.withRequired(true)
				.withShortName("o")
				.withArgument(
						new ArgumentBuilder().withName("output")
								.withMinimum(1).withMaximum(1).create())
				.withDescription("The directory pathname for output.");
	}

	public static DefaultOptionBuilder overwriteOption() {
		return new DefaultOptionBuilder()
				.withLongName("overwrite")
				.withRequired(false)
				.withDescription(
						"If present, overwrite the output directory before running job")
				.withShortName("r");
	}
	
	public static DefaultOptionBuilder fieldSeparatorOption() {
		return (new DefaultOptionBuilder())
				.withLongName("field-separator")
				.withRequired(false)
				.withShortName("s")
				.withArgument(
						(new ArgumentBuilder()).withName("field-separator")
						.withMinimum(1).withMaximum(1).create())
						.withDescription("A character to use for splitting an input record.");
	}

	public static DefaultOptionBuilder baseDateOption() {
		return (new DefaultOptionBuilder())
				.withLongName("base-date")
				.withRequired(true)
				.withShortName("b")
				.withArgument(
						(new ArgumentBuilder()).withName("base-date")
						.withMinimum(1).withMaximum(1).create())
						.withDescription("The date when the data values were collected.");
	}
}
