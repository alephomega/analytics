package com.valuepotion.analytics.core;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

public class LineDataTool {
	private static final long ORIGIN = 946652400000L;
	private static final int DATECODE_RADIX = Character.MAX_RADIX;
	
	private DateFormat dateFormatter;
	private DateFormat timeFormatter;
	private String baseDate;
	private long baseTime;
	private String baseCode;
	
	public LineDataTool(Configuration config) {
		String datePattern = config.get("valuepotion.analytics.date-pattern", "yyyyMMdd");
		String timePattern = config.get("valuepotion.analytics.time-pattern", "yyyyMMdd HHmmss");
		
		this.dateFormatter = new SimpleDateFormat(datePattern);
		this.timeFormatter = new SimpleDateFormat(timePattern);
		this.baseDate = config.get("valuepotion.analytics.base-date", dateFormatter.format(new Date()));
		try {
			this.baseTime = dateFormatter.parse(this.baseDate).getTime();
		} catch (ParseException e) {
			throw new RuntimeException(
					String.format("Illegal value for com.valuepotion.analytics.base-date: %s.", this.baseDate), e);
		}
		
		Calendar calendar = Calendar.getInstance();
		try {
			calendar.setTime(dateFormatter.parse(baseDate));
		} catch (ParseException e) {
			throw new RuntimeException(
					String.format("Invalid parameter value: valuepotion.analytics.base-date (%s)", baseDate));
		}
		
		this.baseCode = Integer.toString((int) TimeUnit.DAYS.convert(this.baseTime - ORIGIN, TimeUnit.MILLISECONDS), DATECODE_RADIX);
	}
	
	public static String[] asFields(String line) {
		return asFields(StringUtils.EMPTY, FieldSeparator.FIELDS, line);
	}

	public static String[] asFields(FieldSeparator fieldSeparator, String line) {
		return asFields(StringUtils.EMPTY, fieldSeparator, line);
	}

	public static String[] asFields(String symbol, String line) {
		return asFields(symbol, FieldSeparator.FIELDS, line);
	}

	public static String[] asFields(String symbol, FieldSeparator fieldSeparator, String line) {
		return StringUtils.splitPreserveAllTokens(line.substring(symbol.length()), fieldSeparator.getSeparator());
	}
	
	public static String elements2Children(String line) {
		return StringUtils.replaceChars(line, FieldSeparator.ELEMENTS.getSeparator(), FieldSeparator.CHILD_ELEMENTS.getSeparator());
	}

	public static String children2GrandChildren(String line) {
		return StringUtils.replaceChars(line, FieldSeparator.CHILD_ELEMENTS.getSeparator(), FieldSeparator.GRANDCHILD_ELEMENTS.getSeparator());
	}

	public static String asLine(Object[] fields) {
		return asLine(StringUtils.EMPTY, FieldSeparator.FIELDS, fields);
	}

	public static String asLine(FieldSeparator fieldSeparator, Object[] fields) {
		return asLine(StringUtils.EMPTY, fieldSeparator, fields);
	}

	public static String asLine(String symbol, Object[] fields) {
		return asLine(symbol, FieldSeparator.FIELDS, fields);
	}

	public static String asLine(String symbol, FieldSeparator fieldSeparator, Object[] fields) {
		return symbol + StringUtils.join(fields, fieldSeparator.getSeparator());
	}
	
	public long asTimeMillis(String dateString) {
		try {
			return dateFormatter.parse(dateString).getTime();
		} catch (ParseException e) {
			throw new IllegalArgumentException(String.format("Illegal value for from: %s.", dateString), e);
		}
	}

	public String asDateString(long time) {
		return dateFormatter.format(new Date(time));
	}
	
	public int diffDays(long to, long from) {
		return (int) TimeUnit.DAYS.convert(to - from, TimeUnit.MILLISECONDS);
	}

	public int diffDays(String to, String from) {
		return diffDays(asTimeMillis(to), asTimeMillis(from));
	}
	
	public int diffDays(String from) {
		return diffDays(baseTime, asTimeMillis(from)) + 1;
	}
	
	public int[] diffDays(String[] dates) {
		if (dates.length == 0) {
			return new int[0];
		}
		
		int[] d = new int[dates.length - 1];
		for (int i = 0; i < dates.length - 1; i++) {
			d[i] = diffDays(dates[i + 1], dates[i]);
		}
		
		return d;
	}
	
	public long diffSecs(String timeString) {
		try {
			return (int) TimeUnit.SECONDS.convert(timeFormatter.parse(timeString).getTime() - baseTime, TimeUnit.MILLISECONDS);
		} catch (ParseException e) {
			throw new IllegalArgumentException(String.format("Illegal value for timeString: %s.", timeString), e);
		}
	}

	public long diffSecs(String to, String from) {
		try {
			return TimeUnit.SECONDS.convert(timeFormatter.parse(to).getTime() - timeFormatter.parse(from).getTime(), TimeUnit.MILLISECONDS);
		} catch (ParseException e) {
			throw new IllegalArgumentException(String.format("Illegal value for to: %s or from: %s.", to, from), e);
		}
	}
	
	public String baseDate() {
		return baseDate;
	}
	
	public long baseTime() {
		return baseTime;
	}
	
	public String baseCode() {
		return baseCode;
	}
	
	public String encodeDate(String dateString) {
		if (isNA(dateString)) {
			return StringUtils.EMPTY;
		}
		
		int d = (int) TimeUnit.DAYS.convert(asTimeMillis(dateString) - ORIGIN, TimeUnit.MILLISECONDS);
		return Integer.toString(d, DATECODE_RADIX);
	}

	public String encodeTime(String timeString) {
		if (isNA(timeString)) {
			return StringUtils.EMPTY;
		}
		
		String d = timeString.substring(0, 8);
		String t = timeString.substring(9);
		
		return encodeDate(d) + Integer.toString(Integer.parseInt(t), DATECODE_RADIX);
	}
	
	public String[] encodeDates(String[] dateStrings) {
		String[] codes = new String[dateStrings.length];
		for (int i = 0; i < dateStrings.length; i++) {
			codes[i] = encodeDate(dateStrings[i]);
		}
		
		return codes;
	}

	public String[] encodeTimes(String[] timeStrings) {
		String[] codes = new String[timeStrings.length];
		for (int i = 0; i < timeStrings.length; i++) {
			codes[i] = encodeTime(timeStrings[i]);
		}
		
		return codes;
	}

	public String decodeDate(String code) {
		if (isNA(code)) {
			return StringUtils.EMPTY;
		}
		
		int d = Integer.parseInt(code, DATECODE_RADIX);
		return dateFormatter.format(new Date(ORIGIN + TimeUnit.MILLISECONDS.convert(d, TimeUnit.DAYS)));
	}
	
	public String decodeTime(String code) {
		if (isNA(code)) {
			return StringUtils.EMPTY;
		}
		
		String d = code.substring(0, 3);
		String t = code.substring(3);

		return decodeDate(d) + String.format(" %06d", Integer.parseInt(t, DATECODE_RADIX));
	}
	
	public String[] decodeDates(String[] codes) {
		String[] dateStrings = new String[codes.length];
		for (int i = 0; i < codes.length; i++) {
			dateStrings[i] = decodeDate(codes[i]);
		}
		
		return dateStrings;
	}
	
	public String[] decodeTimes(String[] codes) {
		String[] timeStrings = new String[codes.length];
		for (int i = 0; i < codes.length; i++) {
			timeStrings[i] = decodeTime(codes[i]);
		}
		
		return timeStrings;
	}

	public int compareDateCodes(String c1, String c2) {
		return c1.compareTo(c2);
	}

	public int compareTimeCodes(String c1, String c2) {
		int r = c1.substring(0, 3).compareTo(c2.substring(0, 3));
		if (r == 0) {
			r = Integer.parseInt(c1.substring(3), DATECODE_RADIX) - Integer.parseInt(c2.substring(3), DATECODE_RADIX);
		}
		
		return r;
	}
	
	
	public String beforeDate(int n) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(baseTime);
		calendar.add(Calendar.DATE, -n);
		
		return dateFormatter.format(calendar.getTime());
	}
	
	public static boolean isNA(String value) {
		return value == null || value.length() == 0;
	}
	
	
	
	public static enum FieldSeparator {
		
		FIELDS('\001'),
		
		ELEMENTS('\002'),
		
		CHILD_ELEMENTS('\003'),

		GRANDCHILD_ELEMENTS('\004'),
		
		GROUPS_OF_ELEMENTS('\005'),
		
		HIERARCHICAL_GROUPS('\006');

		
		private char separator;
		
		FieldSeparator(char separator) {
			this.separator = separator;
		}
		
		public char getSeparator() {
			return separator;
		}
		
		public String getSeparatorString() {
			return String.valueOf(separator);
		}
	}
}
