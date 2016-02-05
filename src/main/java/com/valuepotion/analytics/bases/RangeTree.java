package com.valuepotion.analytics.bases;

import java.util.LinkedList;

public class RangeTree<T> {
	private Node root;
	
	private class Node {
		Interval interval;
		T value;
		Node left, right;
		int N;
		double max;

		Node(Interval interval, T value) {
			this.interval = interval;
			this.value = value;
			this.N = 1;
			this.max = interval.getTo();
		}
	}
	
	public boolean contains(Interval interval) {
        return (get(interval) != null);
    }
	
	public T get(Interval interval) {
        return get(root, interval);
    }

    private T get(Node x, Interval interval) {
        if (x == null) {
        	return null;
        }
        
        int cmp = interval.compareTo(x.interval);
        if (cmp < 0) {
        	return get(x.left, interval);
        } else if (cmp > 0) {
        	return get(x.right, interval);
        } else {
        	return x.value;
        }
    }
    
	public void put(Interval interval, T value) {
		if (contains(interval)) {
			remove(interval);
		}
		
		root = randomizedInsert(root, interval, value);
	}

	private Node randomizedInsert(Node x, Interval interval, T value) {
		if (x == null) {
			return new Node(interval, value);
		}
		
		if (Math.random() * size(x) < 1.0) {
			return rootInsert(x, interval, value);
		}

		int cmp = interval.compareTo(x.interval);
		if (cmp < 0) {
			x.left = randomizedInsert(x.left, interval, value);
		} else {
			x.right = randomizedInsert(x.right, interval, value);
		}
		fix(x);
		return x;
	}

	private Node rootInsert(Node x, Interval interval, T value) {
		if (x == null) {
			return new Node(interval, value);
		}
		
		int cmp = interval.compareTo(x.interval);
		if (cmp < 0) {
			x.left = rootInsert(x.left, interval, value);
			x = rotR(x);
		} else {
			x.right = rootInsert(x.right, interval, value);
			x = rotL(x);
		}
		return x;
	}

	private Node joinLR(Node a, Node b) {
		if (a == null) {
			return b;
		}
		
		if (b == null) {
			return a;
		}

		if (Math.random() * (size(a) + size(b)) < size(a)) {
			a.right = joinLR(a.right, b);
			fix(a);
			return a;
		} else {
			b.left = joinLR(a, b.left);
			fix(b);
			return b;
		}
	}

    public T remove(Interval interval) {
        T value = get(interval);
        root = remove(root, interval);
        return value;
    }

	private Node remove(Node h, Interval interval) {
		if (h == null) {
			return null;
		}

		int cmp = interval.compareTo(h.interval);
		if (cmp < 0) {
			h.left = remove(h.left, interval);
		} else if (cmp > 0) {
			h.right = remove(h.right, interval);
		} else {
			h = joinLR(h.left, h.right);
		}

		fix(h);
		return h;
	}

	public Interval search(Interval interval) {
		return search(root, interval);
	}

	public Interval search(Node x, Interval interval) {
		while (x != null) {
			if (interval.intersects(x.interval)) {
				return x.interval;
			} else if (x.left == null) {
				x = x.right;
			} else if (x.left.max < interval.getFrom()) {
				x = x.right;
			} else {
				x = x.left;
			}
		}
		return null;
	}

	public Iterable<Interval> searchAll(Interval interval) {
		LinkedList<Interval> list = new LinkedList<Interval>();
		searchAll(root, interval, list);
		return list;
	}

	public boolean searchAll(Node x, Interval interval, LinkedList<Interval> list) {
		boolean found1 = false;
		boolean found2 = false;
		boolean found3 = false;

		if (x == null) {
			return false;
		}
		if (interval.intersects(x.interval)) {
			list.add(x.interval);
			found1 = true;
		}
		if (x.left != null && x.left.max > interval.getFrom()) {
			found2 = searchAll(x.left, interval, list);
		}
		if (found2 || x.left == null || x.left.max <= interval.getFrom()) {
			found3 = searchAll(x.right, interval, list);
		}
		return found1 || found2 || found3;
	}
	
	public int size() {
		return size(root);
	}

	private int size(Node x) {
		if (x == null) {
			return 0;
		} else {
			return x.N;
		}
	}

	public int height() {
		return height(root);
	}

	private int height(Node x) {
		if (x == null) {
			return 0;
		}
		return 1 + Math.max(height(x.left), height(x.right));
	}

	private void fix(Node x) {
		if (x == null) {
			return;
		}
		x.N = 1 + size(x.left) + size(x.right);
		x.max = max3(x.interval.getTo(), max(x.left), max(x.right));
	}

	private double max(Node x) {
		if (x == null) {
			return Integer.MIN_VALUE;
		}

		return x.max;
	}

	private double max3(double a, double b, double c) {
		return Math.max(a, Math.max(b, c));
	}

	private Node rotR(Node h) {
		Node x = h.left;
		h.left = x.right;
		x.right = h;
		fix(h);
		fix(x);
		return x;
	}

	private Node rotL(Node h) {
		Node x = h.right;
		h.right = x.left;
		x.left = h;
		fix(h);
		fix(x);
		return x;
	}
}
