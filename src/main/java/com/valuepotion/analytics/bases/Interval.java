package com.valuepotion.analytics.bases;

public class Interval implements Comparable<Interval> {
	private double from;
	private double to;

	public Interval(double from, double to) {
		if (from < to) {
			this.from = from;
			this.to = to;
		} else {
			throw new RuntimeException("Illegal interval");
		}
	}

	public double getFrom() {
		return from;
	}

	public double getTo() {
		return to;
	}

	public boolean intersects(Interval that) {
		if (that.to <= this.from) {
			return false;
		}
		
		if (this.to <= that.from) {
			return false;
		}
		return true;
	}

	public boolean contains(int x) {
		return (from <= x) && (x <= to);
	}

	public int compareTo(Interval that) {
		if (this.from < that.from) {
			return -1;
		} else if (this.from > that.from) {
			return +1;
		} else if (this.to < that.to) {
			return -1;
		} else if (this.to > that.to) {
			return +1;
		} else {
			return 0;
		}
	}

	public String toString() {
		return "[" + from + ", " + to + "]";
	}
}
