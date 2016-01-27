package com.valuepotion.analytics.bases;

import java.util.LinkedList;

import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.serializers.Serializer;


public abstract class LineBuilder<E> {
	
	public static interface Filter<E> {
		void add(E element);
		E apply();
	}
	
	protected LinkedList<Filter<E>> partitions = new LinkedList<Filter<E>>();
	protected Serializer<E> serializer;
	protected LineDataTool dataTool;

	protected LineBuilder(Serializer<E> serializer, LineDataTool dataTool) {
		this.serializer = serializer;
		this.dataTool = dataTool;
		this.partitions.add(initFilter());
	}
	
	public void add(String line) {
		partitions.getLast().add(serializer.deserialize(line, dataTool));
	}

	public void onAttributionChanged() {
		partitions.add(initFilter());
	}
	
	public String[] build() {
		String[] lines = new String[partitions.size()];
		
		for (int i = 0; i < partitions.size() ; i++) {
			Filter<E> attribution = partitions.get(i);
			lines[i] = serializer.serialize(attribution.apply(), dataTool);
		}
		
		return lines;
	}
	
	public abstract E[] get();
	
	public abstract Filter<E> initFilter();
}
