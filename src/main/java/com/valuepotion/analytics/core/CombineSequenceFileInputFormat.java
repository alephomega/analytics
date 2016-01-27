package com.valuepotion.analytics.core;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

public class CombineSequenceFileInputFormat<K, V> extends CombineFileInputFormat<K, V> {
	public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new CombineFileRecordReader((CombineFileSplit) split, context, SequenceFileRecordReaderWrapper.class);
	}

	private static class SequenceFileRecordReaderWrapper<K, V> extends CombineFileRecordReaderWrapper<K, V> {
		public SequenceFileRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer idx) throws IOException, InterruptedException {
			super(new SequenceFileInputFormat<K, V>(), split, context, idx);
		}
	}
}
