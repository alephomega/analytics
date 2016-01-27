package com.valuepotion.analytics.core;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;

public class CombineKeyValueTextInputFormat extends CombineFileInputFormat<Text, Text> {

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
		return new CombineKeyValueLineRecordReader();
	}

	public static class CombineKeyValueLineRecordReader extends RecordReader<Text, Text> {

		private CombineFileSplit split = null;
		private int currentSplit = 0;
		private TaskAttemptContext context = null;
		private KeyValueLineRecordReader reader = null;

		
		@Override
		public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
			this.context = context;
			split = (CombineFileSplit) inputSplit;

			if (split.getLength() == 0) {
				// ?
			}
			
			initializeNextReader();
		}

		@Override
		public boolean nextKeyValue() throws IOException {

			do {
				if (reader.nextKeyValue()) {
					return true;
				} else if (currentSplit < split.getNumPaths()) {
					initializeNextReader();
				} else {
					return false;
				}

			} while (true);
		}

		@Override
		public void close() throws IOException {
			reader.close();
		}

		@Override
		public Text getCurrentKey() {
			return reader.getCurrentKey();
		}

		@Override
		public Text getCurrentValue() {
			return reader.getCurrentValue();
		}

		@Override
		public float getProgress() {
			return (currentSplit - 1 + reader.getProgress()) / split.getNumPaths();
		}

		private void initializeNextReader() throws IOException {
			reader = new KeyValueLineRecordReader(context.getConfiguration());
			reader.initialize(new FileSplit(split.getPath(currentSplit), split.getOffset(currentSplit), split.getLength(currentSplit), null), context);
			++currentSplit;
		}
	}
}
