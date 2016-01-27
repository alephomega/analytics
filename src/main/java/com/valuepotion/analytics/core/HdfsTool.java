package com.valuepotion.analytics.core;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsTool {

	public static void delete(Configuration conf, Iterable<Path> paths) throws IOException {
		if (conf == null) {
			conf = new Configuration();
		}
		
		for (Path path : paths) {
			FileSystem fs = path.getFileSystem(conf);
			if (fs.exists(path)) {
				fs.delete(path, true);
			}
		}
	}

	public static void delete(Configuration conf, Path... paths) throws IOException {
		delete(conf, Arrays.asList(paths));
	}
}
