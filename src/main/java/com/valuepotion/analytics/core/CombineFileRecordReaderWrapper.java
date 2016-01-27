package com.valuepotion.analytics.core;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public abstract class CombineFileRecordReaderWrapper<K, V> extends RecordReader<K, V> {
	private final FileSplit fileSplit;
	private final RecordReader<K, V> delegate;

	protected CombineFileRecordReaderWrapper(FileInputFormat<K, V> inputFormat, CombineFileSplit split, TaskAttemptContext context, Integer idx) throws IOException, InterruptedException {
		fileSplit = new FileSplit(split.getPath(idx), split.getOffset(idx), split.getLength(idx), split.getLocations());
		delegate = inputFormat.createRecordReader(fileSplit, context);
	}

	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		delegate.initialize(fileSplit, context);
	}

	public boolean nextKeyValue() throws IOException, InterruptedException {
		return delegate.nextKeyValue();
	}

	public K getCurrentKey() throws IOException, InterruptedException {
		return delegate.getCurrentKey();
	}

	public V getCurrentValue() throws IOException, InterruptedException {
		return delegate.getCurrentValue();
	}

	public float getProgress() throws IOException, InterruptedException {
		return delegate.getProgress();
	}

	public void close() throws IOException {
		delegate.close();
	}
}
