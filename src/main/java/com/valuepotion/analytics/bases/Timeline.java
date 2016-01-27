package com.valuepotion.analytics.bases;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.valuepotion.analytics.core.LineDataTool;
import com.valuepotion.analytics.events.Event;

public class Timeline {
	
	class Timer {
		private String d0;
		private String t0;
		
		Timer(String d0, String t0) {
			this.d0 = d0;
			this.t0 = t0;
		}
		
		double getTime(String s) {
			return dataTool.diffSecs(s, t0) / (double) (60 * 60 * 24);
		}
		
		double[] getTimes(String[] ss) {
			double[] ts = new double[ss.length];
			for (int i = 0; i < ss.length; i++) {
				ts[i] = getTime(ss[i]);
			}
			
			return ts;
		}
		
		double getDate(String d) {
			return (double) dataTool.diffDays(d, d0);
		}
	}
	
	class TimeBlock {
		private Interval interval;
		private TreeSet<Event> events;
		
		TimeBlock(Interval interval) {
			this.interval = interval;
			this.events = new TreeSet<Event>();
		}
		
		Interval getInterval() {
			return interval;
		}
		
		TreeSet<Event> getEvents() {
			return events;
		}
	}
	
	private Timer timer;
	List<TimeBlock> blocks;
	
	private RangeTree<TreeSet> searchTree = new RangeTree<TreeSet>();
	private LineDataTool dataTool;
	
	public Timeline(String d0, String t0, String[] blockpoints, LineDataTool dataTool) {
		this.dataTool = dataTool;
		this.timer = new Timer(d0, t0);

		setTimeBlocks(blockpoints);
	}
	
	void setTimeBlocks(String[] blockpoints) {
		blocks = new ArrayList<TimeBlock>(blockpoints == null ? 1 : blockpoints.length + 1);

		if (blockpoints == null || blockpoints.length == 0) {
			TimeBlock block = new TimeBlock(new Interval(0d, (double) Integer.MAX_VALUE));
			searchTree.put(block.interval, block.events);

			blocks.add(block);

		} else {
			
			double[] points = Arrays.copyOf(timer.getTimes(blockpoints), blockpoints.length + 1);
			points[points.length - 1] = (double) Integer.MAX_VALUE;
			
			double f = -1d;
			for (double t : points) {
				if (f == t) {
					blocks.add(new TimeBlock(null));
					continue;
				}
				
				TimeBlock block = new TimeBlock(new Interval(f, t));
				
				searchTree.put(block.interval, block.events);
				blocks.add(block);
				f = t;
			}
		}
	}
	
	public void addEvent(int block, Event event) {
		if (block >= 0 && block < blocks.size()) {
			blocks.get(block).events.add(event);
		}
	}
	

	public Iterator<Event> iterator(Event from, Event to) {
		return getEvents(from, to).iterator();
	}

	public List<Event> getEvents(Event from, Event to) {
		List<Event> res = new ArrayList<Event>();
		
		double f = timer.getDate(from.getDate());
		double t = timer.getDate(to.getDate());
		
		if (f <= t) {
			Iterable<Interval> intervals = searchTree.searchAll(new Interval(f-1d, t+1d));
			
			for (Interval interval : intervals) {
				TreeSet<Event> events = searchTree.get(interval);
				res.addAll(events.subSet(from, true, to, true));
			}
		}
		
		return res;
	}
	
	public int getCount(Event from, Event to) {

		Set<Event> set = new HashSet<Event>();
		
		double f = timer.getDate(from.getDate());
		double t = timer.getDate(to.getDate());
		
		if (f <= t) {
			Iterable<Interval> intervals = searchTree.searchAll(new Interval(f-1d, t+1d));
			
			for (Interval interval : intervals) {
				TreeSet<Event> events = searchTree.get(interval);
				set.addAll(events.subSet(from, true, to, true));
			}
		}
		
		return set.size();
	}
	
	
	public Iterator<Event> iterator(Event from) {
		return getEvents(from).iterator();
	}
	
	public List<Event> getEvents(Event from) {
		List<Event> res = new ArrayList<Event>();
		
		double f = timer.getDate(from.getDate());
		double t = (double) Integer.MAX_VALUE;
				
		Iterable<Interval> intervals = searchTree.searchAll(new Interval(f-1d, t));
		
		for (Interval interval : intervals) {
			TreeSet<Event> events = (TreeSet<Event>) searchTree.get(interval);
			res.addAll(events.tailSet(from, true));
		}
		
		return res;
	}
	
	public int getCount(Event from) {
		Set<Event> set = new HashSet<Event>();
		
		double f = timer.getDate(from.getDate());
		double t = (double) Integer.MAX_VALUE;
		
		Iterable<Interval> intervals = searchTree.searchAll(new Interval(f-1d, t));
		
		for (Interval interval : intervals) {
			TreeSet<Event> events = (TreeSet<Event>) searchTree.get(interval);
			set.addAll(events.tailSet(from, true));
		}
		
		return set.size();
	}
	
	public String getDate0() {
		return timer.d0;
	}
	
	public String getTime0() {
		return timer.t0;
	}
}
