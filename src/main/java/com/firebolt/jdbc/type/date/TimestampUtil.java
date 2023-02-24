/*
 * Copyright 2023 Firebolt Analytics, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * NOTICE: THIS FILE HAS BEEN MODIFIED BY Firebolt Analytics, Inc. from the PostgreSQL Database Management System repository
 * URLs :
 *  - Repository: https://github.com/postgres/postgres/
 * Changes:
 *  - Class and file name
 *  - Comments and documentation
 *  - imported DATE_POSITIVE_INFINITY and DATE_NEGATIVE_INFINITY from PGStatement
 *  - Imports
 *  - Package name
 *  - Formatting
 *  - Some variable names
 *  - Some methods logic (eg: added synchronized blocks, etc)
 *  - Class members have been reorganized
 *  - Access modifiers and non-access modifiers
 *  - Class name from TimestampUtils to TimestampUtil
 *  - Type specification in the map constructor with the diamond operator
 *  - Adding final keyword for constants
 *  - Replacing isSpace() with isWhiteSpaced()
 */

/*
 *  PostgreSQL Database Management System
 *  (formerly known as Postgres, then as Postgres95)
 *  Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *  Portions Copyright (c) 1994, The Regents of the University of California
 *  Permission to use, copy, modify, and distribute this software and its
 *  documentation for any purpose, without fee, and without a written agreement
 *  is hereby granted, provided that the above copyright notice and this
 *  paragraph and the following two paragraphs appear in all copies.
 *
 *  IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 *  DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 *  LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 *  DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 *  POSSIBILITY OF SUCH DAMAGE.
 *
 *  THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 *  INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 *  AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 *  ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 *  PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 */

/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */
package com.firebolt.jdbc.type.date;

import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.*;

import javax.annotation.Nullable;

import com.firebolt.jdbc.JavaVersion;
import com.firebolt.jdbc.exception.FireboltException;

import lombok.experimental.UtilityClass;

@UtilityClass
public class TimestampUtil {

	private static final char[][] NUMBERS;
	private static final HashMap<String, TimeZone> GMT_ZONES = new HashMap<>();
	private static final long DATE_POSITIVE_INFINITY = 9223372036825200000L;
	private static final long DATE_NEGATIVE_INFINITY = -9223372036832400000L;
	private static final @Nullable Field DEFAULT_TIME_ZONE_FIELD;
	private static final int ONEDAY = 24 * 3600 * 1000;
	private static @Nullable TimeZone prevDefaultZoneFieldValue;
	private static @Nullable TimeZone defaultTimeZoneCache;
	// This calendar is used when user provides calendar in setX(, Calendar) method.
	// It ensures calendar is Gregorian.
	private static final Calendar calendarWithUserTz = new GregorianCalendar();
	private static @Nullable Calendar calCache;
	private static @Nullable ZoneOffset calCacheZone;

	static {
		// The expected maximum value is 60 (seconds), so 64 is used "just in case"
		NUMBERS = new char[64][];
		for (int i = 0; i < NUMBERS.length; i++) {
			NUMBERS[i] = ((i < 10 ? "0" : "") + Integer.toString(i)).toCharArray();
		}

		// Backend's gmt-3 means GMT+03 in Java. Here a map is created so gmt-3 can be
		// converted to
		// java TimeZone
		for (int i = -12; i <= 14; i++) {
			TimeZone timeZone;
			String pgZoneName;
			if (i == 0) {
				timeZone = TimeZone.getTimeZone("GMT");
				pgZoneName = "GMT";
			} else {
				timeZone = TimeZone.getTimeZone("GMT" + (i <= 0 ? "+" : "-") + Math.abs(i));
				pgZoneName = "GMT" + (i >= 0 ? "+" : "-");
			}

			if (i == 0) {
				GMT_ZONES.put(pgZoneName, timeZone);
				continue;
			}
			GMT_ZONES.put(pgZoneName + Math.abs(i), timeZone);
			GMT_ZONES.put(pgZoneName + new String(NUMBERS[Math.abs(i)]), timeZone);
		}
		// Fast path to getting the default timezone.
		// Accessing the default timezone over and over creates a clone with regular
		// API.
		// Because we don't mutate that object in our use of it, we can access the field
		// directly.
		// This saves the creation of a clone everytime, and the memory associated to
		// all these clones.
		Field tzField;
		try {
			tzField = null;
			// Avoid reflective access in Java 9+
			if (JavaVersion.getRuntimeVersion().compareTo(JavaVersion.V1_8) <= 0) {
				tzField = TimeZone.class.getDeclaredField("defaultTimeZone");
				tzField.setAccessible(true);
				TimeZone defaultTz = getDefaultTz();
				Object tzFromField = tzField.get(null);
				if (defaultTz == null || !defaultTz.equals(tzFromField)) {
					tzField = null;
				}
			}
		} catch (Exception e) {
			tzField = null;
		}
		DEFAULT_TIME_ZONE_FIELD = tzField;
	}

	/**
	 * Extracts the date part from a timestamp.
	 *
	 * @param millis The timestamp from which to extract the date.
	 * @param tz     The time zone of the date.
	 * @return The extracted date.
	 */
	public static Date convertToDate(long millis, @Nullable TimeZone tz) {

		// no adjustments for the inifity hack values
		if (millis <= DATE_NEGATIVE_INFINITY || millis >= DATE_POSITIVE_INFINITY) {
			return new Date(millis);
		}
		if (tz == null) {
			tz = getDefaultTz();
		}
		if (isSimpleTimeZone(tz.getID())) {
			// Truncate to 00:00 of the day.
			// Suppose the input date is 7 Jan 15:40 GMT+02:00 (that is 13:40 UTC)
			// We want it to become 7 Jan 00:00 GMT+02:00
			// 1) Make sure millis becomes 15:40 in UTC, so add offset
			int offset = tz.getRawOffset();
			millis += offset;
			// 2) Truncate hours, minutes, etc. Day is always 86400 seconds, no matter what
			// leap seconds
			// are
			millis = floorDiv(millis, ONEDAY) * ONEDAY;
			// 2) Now millis is 7 Jan 00:00 UTC, however we need that in GMT+02:00, so
			// subtract some
			// offset
			millis -= offset;
			// Now we have brand-new 7 Jan 00:00 GMT+02:00
			return new Date(millis);
		}
		synchronized (calendarWithUserTz) {
			Calendar cal = calendarWithUserTz;
			cal.setTimeZone(tz);
			cal.setTimeInMillis(millis);
			cal.set(Calendar.HOUR_OF_DAY, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			cal.set(Calendar.MILLISECOND, 0);
			return new Date(cal.getTimeInMillis());
		}
	}

	/**
	 * Parse a string and return a time representing its value.
	 *
	 * @param s  The ISO formated time string to parse.
	 * @param tz timezone
	 * @return null if s is null or a time of the parsed string s.
	 * @throws SQLException if there is a problem parsing s.
	 */
	public static Time toTime(String s, TimeZone tz) throws SQLException {
		// 1) Parse backend string
		if (s == null) {
			return null;
		}
		ParsedTimestamp ts = parseBackendTimestamp(s);
		long timeMillis;

		Calendar calendar = ts.hasOffset ? getCalendar(ts.offset) : calendarWithUserTz;
		synchronized (calendar) {
			if (!ts.hasOffset) {
				if (tz == null) {
					tz = getDefaultTz();
				}
				// When no time zone provided (e.g. time or timestamp)
				// We get the year-month-day from the string, then truncate the day to
				// 1970-01-01
				// This is used for timestamp -> time conversion
				// Note: this cannot be merged with "else" branch since
				// timestamps at which the time flips to/from DST depend on the date
				// For instance, 2000-03-26 02:00:00 is invalid timestamp in Europe/Moscow time
				// zone
				// and the valid one is 2000-03-26 03:00:00. That is why we parse full timestamp
				// then set year to 1970 later
				calendar.setTimeZone(tz);
				calendar.set(Calendar.ERA, ts.era);
				calendar.set(Calendar.YEAR, ts.year);
				calendar.set(Calendar.MONTH, ts.month - 1);
				calendar.set(Calendar.DAY_OF_MONTH, ts.day);
			} else {
				// When time zone is given, we just pick the time part and assume date to be
				// 1970-01-01
				// this is used for time, timez, and timestamptz parsing
				calendar.set(Calendar.ERA, GregorianCalendar.AD);
				calendar.set(Calendar.YEAR, 1970);
				calendar.set(Calendar.MONTH, Calendar.JANUARY);
				calendar.set(Calendar.DAY_OF_MONTH, 1);
			}
			calendar.set(Calendar.HOUR_OF_DAY, ts.hour);
			calendar.set(Calendar.MINUTE, ts.minute);
			calendar.set(Calendar.SECOND, ts.second);
			calendar.set(Calendar.MILLISECOND, 0);

			timeMillis = calendar.getTimeInMillis() + ts.nanos / 1000000;

			if (ts.hasOffset || (ts.year == 1970 && ts.era == GregorianCalendar.AD)) {
				// time with time zone has proper time zone, so the value can be returned as is
				return new Time(timeMillis);
			}
			// 2) Truncate date part so in given time zone the date would be formatted as
			// 01/01/1970
			return convertToTime(timeMillis, calendar.getTimeZone());
		}
	}

	/**
	 * Parse a string and return a timestamp representing its value.
	 *
	 * @param s  The ISO formated date string to parse.
	 * @param tz timezone
	 * @return null if s is null or a timestamp of the parsed string s.
	 * @throws SQLException if there is a problem parsing s.
	 */
	public static Timestamp toTimestamp(String s, @Nullable TimeZone tz) throws SQLException {
		if (s == null) {
			return null;
		}

		int slen = s.length();

		// convert postgres's infinity values to internal infinity magic value
		if (slen == 8 && s.equals("infinity")) {
			return new Timestamp(DATE_POSITIVE_INFINITY);
		}

		if (slen == 9 && s.equals("-infinity")) {
			return new Timestamp(DATE_NEGATIVE_INFINITY);
		}
		ParsedTimestamp ts = parseBackendTimestamp(s);
		Calendar useCal = ts.hasOffset ? getCalendar(ts.offset) : calendarWithUserTz;
		synchronized (useCal) {
			if (!ts.hasOffset) {
				if (tz == null) {
					tz = getDefaultTz();
				}
				useCal.setTimeZone(tz);
			}
			useCal.set(Calendar.ERA, ts.era);
			useCal.set(Calendar.YEAR, ts.year);
			useCal.set(Calendar.MONTH, ts.month - 1);
			useCal.set(Calendar.DAY_OF_MONTH, ts.day);
			useCal.set(Calendar.HOUR_OF_DAY, ts.hour);
			useCal.set(Calendar.MINUTE, ts.minute);
			useCal.set(Calendar.SECOND, ts.second);
			useCal.set(Calendar.MILLISECOND, 0);

			Timestamp result = new Timestamp(useCal.getTimeInMillis());
			result.setNanos(ts.nanos);
			return result;
		}
	}

	private static long floorDiv(long x, long y) {
		long r = x / y;
		// if the signs are different and modulo not zero, round down
		if ((x ^ y) < 0 && (r * y != x)) {
			r--;
		}
		return r;
	}

	private static boolean isSimpleTimeZone(String id) {
		return id.startsWith("GMT") || id.startsWith("UTC");
	}

	private static int skipWhitespace(char[] s, int start) {
		int slen = s.length;
		for (int i = start; i < slen; i++) {
			if (!Character.isWhitespace(s[i])) {
				return i;
			}
		}
		return slen;
	}

	private static int firstNonDigit(char[] s, int start) {
		int slen = s.length;
		for (int i = start; i < slen; i++) {
			if (!Character.isDigit(s[i])) {
				return i;
			}
		}
		return slen;
	}

	private static char charAt(char[] s, int pos) {
		if (pos >= 0 && pos < s.length) {
			return s[pos];
		}
		return '\0';
	}

	private static int number(char[] s, int start, int end) {
		if (start >= end) {
			throw new NumberFormatException();
		}
		int n = 0;
		for (int i = start; i < end; i++) {
			n = 10 * n + (s[i] - '0');
		}
		return n;
	}

	private static long floorMod(long x, long y) {
		return x - floorDiv(x, y) * y;
	}

	/**
	 * Converts millis to time. This method ensures the date part of
	 * output timestamp looks like 1970-01-01 in given timezone.
	 *
	 * @param millis The timestamp from which to extract the time.
	 * @param tz     timezone to use.
	 * @return The extracted time.
	 */
	private static Time convertToTime(long millis, TimeZone tz) {
		if (tz == null) {
			tz = getDefaultTz();
		}
		if (isSimpleTimeZone(tz.getID())) {
			// Leave just time part of the day.
			// Suppose the input date is 2015 7 Jan 15:40 GMT+02:00 (that is 13:40 UTC)
			// We want it to become 1970 1 Jan 15:40 GMT+02:00
			// 1) Make sure millis becomes 15:40 in UTC, so add offset
			int offset = tz.getRawOffset();
			millis += offset;
			// 2) Truncate year, month, day. Day is always 86400 seconds, no matter what
			// leap seconds are
			millis = floorMod(millis, ONEDAY);
			// 2) Now millis is 1970 1 Jan 15:40 UTC, however we need that in GMT+02:00, so
			// subtract some
			// offset
			millis -= offset;
			// Now we have brand-new 1970 1 Jan 15:40 GMT+02:00
			return new Time(millis);
		}
		synchronized (calendarWithUserTz) {
			Calendar cal = calendarWithUserTz;
			cal.setTimeZone(tz);
			cal.setTimeInMillis(millis);
			cal.set(Calendar.ERA, GregorianCalendar.AD);
			cal.set(Calendar.YEAR, 1970);
			cal.set(Calendar.MONTH, 0);
			cal.set(Calendar.DAY_OF_MONTH, 1);
			return new Time(cal.getTimeInMillis());
		}
	}

	private static synchronized Calendar getCalendar(ZoneOffset offset) {
		if (calCache != null && Objects.equals(offset, calCacheZone)) {
			return calCache;
		}

		// normally we would use:
		// calCache = new GregorianCalendar(TimeZone.getTimeZone(offset));
		// But this seems to cause issues for some crazy offsets as returned by server
		// for BC dates!
		final String tzid = (offset.getTotalSeconds() == 0) ? "UTC" : "GMT".concat(offset.getId());
		final TimeZone syntheticTZ = new SimpleTimeZone(offset.getTotalSeconds() * 1000, tzid);
		calCache = new GregorianCalendar(syntheticTZ);
		calCacheZone = offset;
		return calCache;
	}

	private static TimeZone getDefaultTz() {
		// Fast path to getting the default timezone.
		if (DEFAULT_TIME_ZONE_FIELD != null) {
			try {
				TimeZone defaultTimeZone = (TimeZone) DEFAULT_TIME_ZONE_FIELD.get(null);
				if (defaultTimeZoneCache != null) {
					if (defaultTimeZone == prevDefaultZoneFieldValue) {
						return defaultTimeZoneCache;
					}
					prevDefaultZoneFieldValue = defaultTimeZone;
				}
			} catch (Exception e) {
				// If this were to fail, fallback on slow method.
			}
		}
		TimeZone tz = TimeZone.getDefault();
		defaultTimeZoneCache = tz;
		return tz;
	}

	private static class ParsedTimestamp {
		boolean hasDate = false;
		int era = GregorianCalendar.AD;
		int year = 1970;
		int month = 1;

		boolean hasTime = false;
		int day = 1;
		int hour = 0;
		int minute = 0;
		int second = 0;
		int nanos = 0;

		boolean hasOffset = false;
		ZoneOffset offset = ZoneOffset.UTC;
	}

	/**
	 * Load date/time information into the provided calendar returning the
	 * fractional seconds.
	 */
	private static ParsedTimestamp parseBackendTimestamp(String str) throws SQLException {
		char[] s = str.toCharArray();
		int slen = s.length;

		// This is pretty gross..
		ParsedTimestamp result = new ParsedTimestamp();

		// We try to parse these fields in order; all are optional
		// (but some combinations don't make sense, e.g. if you have
		// both date and time then they must be whitespace-separated).
		// At least one of date and time must be present.

		// leading whitespace
		// yyyy-mm-dd
		// whitespace
		// hh:mm:ss
		// whitespace
		// timezone in one of the formats: +hh, -hh, +hh:mm, -hh:mm
		// whitespace
		// if date is present, an era specifier: AD or BC
		// trailing whitespace

		try {
			int start = skipWhitespace(s, 0); // Skip leading whitespace
			int end = firstNonDigit(s, start);
			int num;
			char sep;

			// Possibly read date.
			if (charAt(s, end) == '-') {
				//
				// Date
				//
				result.hasDate = true;

				// year
				result.year = number(s, start, end);
				start = end + 1; // Skip '-'

				// month
				end = firstNonDigit(s, start);
				result.month = number(s, start, end);

				sep = charAt(s, end);
				if (sep != '-') {
					throw new NumberFormatException("Expected date to be dash-separated, got '" + sep + "'");
				}

				start = end + 1; // Skip '-'

				// day of month
				end = firstNonDigit(s, start);
				result.day = number(s, start, end);

				start = skipWhitespace(s, end); // Skip trailing whitespace
			}

			// Possibly read time.
			if (Character.isDigit(charAt(s, start))) {
				//
				// Time.
				//

				result.hasTime = true;

				// Hours

				end = firstNonDigit(s, start);
				result.hour = number(s, start, end);

				sep = charAt(s, end);
				if (sep != ':') {
					throw new NumberFormatException("Expected time to be colon-separated, got '" + sep + "'");
				}

				start = end + 1; // Skip ':'

				// minutes

				end = firstNonDigit(s, start);
				result.minute = number(s, start, end);

				sep = charAt(s, end);
				if (sep != ':') {
					throw new NumberFormatException("Expected time to be colon-separated, got '" + sep + "'");
				}

				start = end + 1; // Skip ':'

				// seconds

				end = firstNonDigit(s, start);
				result.second = number(s, start, end);
				start = end;

				// Fractional seconds.
				if (charAt(s, start) == '.') {
					end = firstNonDigit(s, start + 1); // Skip '.'
					num = number(s, start + 1, end);

					for (int numlength = (end - (start + 1)); numlength < 9; ++numlength) {
						num *= 10;
					}

					result.nanos = num;
					start = end;
				}

				start = skipWhitespace(s, start); // Skip trailing whitespace
			}

			// Possibly read timezone.
			sep = charAt(s, start);
			if (sep == '-' || sep == '+') {
				result.hasOffset = true;

				int tzsign = (sep == '-') ? -1 : 1;
				int tzhr;
				int tzmin;
				int tzsec;

				end = firstNonDigit(s, start + 1); // Skip +/-
				tzhr = number(s, start + 1, end);
				start = end;

				sep = charAt(s, start);
				if (sep == ':') {
					end = firstNonDigit(s, start + 1); // Skip ':'
					tzmin = number(s, start + 1, end);
					start = end;
				} else {
					tzmin = 0;
				}

				tzsec = 0;
				sep = charAt(s, start);
				if (sep == ':') {
					end = firstNonDigit(s, start + 1); // Skip ':'
					tzsec = number(s, start + 1, end);
					start = end;
				}

				result.offset = ZoneOffset.ofHoursMinutesSeconds(tzsign * tzhr, tzsign * tzmin, tzsign * tzsec);

				start = skipWhitespace(s, start); // Skip trailing whitespace
			}

			if (result.hasDate && start < slen) {
				String eraString = new String(s, start, slen - start);
				if (eraString.startsWith("AD")) {
					result.era = GregorianCalendar.AD;
					start += 2;
				} else if (eraString.startsWith("BC")) {
					result.era = GregorianCalendar.BC;
					start += 2;
				}
			}

			if (start < slen) {
				throw new NumberFormatException(
						"Trailing junk on timestamp: '" + new String(s, start, slen - start) + "'");
			}

			if (!result.hasTime && !result.hasDate) {
				throw new NumberFormatException("Timestamp has neither date nor time");
			}

		} catch (NumberFormatException nfe) {
			throw new FireboltException(String.format("Bad value for type timestamp/date/time: %s", str));
		}

		return result;
	}
}
