package com.firebolt.jdbc.type.array;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.column.ColumnType;
import com.firebolt.jdbc.type.FireboltDataType;
import com.firebolt.jdbc.type.JavaTypeToFireboltSQLString;
import com.firebolt.jdbc.util.StringUtil;
import lombok.CustomLog;
import lombok.NonNull;

import javax.annotation.Nullable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@CustomLog
public class SqlArrayUtil {
	private static final Map<Character, Markers> formatMarkers = Map.of(
			'[', new Markers('[', ']', '\'', '\''),
			'{', new Markers('{', '}', '"', '\'')
	);
	private final ColumnType columnType;
	private final Markers markers;
	public static final String BYTE_ARRAY_PREFIX = "\\x";

	private static final class Markers {
		private final char leftArrayBracket;
		private final char rightArrayBracket;
		private final char literalQuote;
		private final char tupleLiteralQuote;

		public Markers(char leftArrayBracket, char rightArrayBracket, char literalQuote, char tupleLiteralQuote) {
			this.leftArrayBracket = leftArrayBracket;
			this.rightArrayBracket = rightArrayBracket;
			this.literalQuote = literalQuote;
			this.tupleLiteralQuote = tupleLiteralQuote;
		}
	}

    public SqlArrayUtil(ColumnType columnType, Markers markers) {
        this.columnType = columnType;
        this.markers  = markers;
    }

    public static FireboltArray transformToSqlArray(String value, ColumnType columnType) throws SQLException {
		log.debug("Transformer array with value {} and type {}", value, columnType);
		if (isNullValue(value))  {
			return null;
		}
		int dimensions = getDimensions(columnType);
		SqlArrayUtil parser = new SqlArrayUtil(columnType,
				ofNullable(formatMarkers.get(value.charAt(0))).orElseThrow(() -> new IllegalArgumentException("Wrong format"))
		);
		Object arr = parser.createArray(value, dimensions);
		return arr == null ? null : new FireboltArray(columnType.getArrayBaseColumnType().getDataType(), arr);
	}

	private static int getDimensions(ColumnType columnType) {
		int dimensions = 0;
		for (ColumnType type = columnType; FireboltDataType.ARRAY.equals(type.getDataType()); type = type.getInnerTypes().get(0)) {
			dimensions++;
		}
		return dimensions;
	}

	private Object createArray(String arrayContent, int dimension) throws SQLException {
		int from = arrayContent.charAt(0) == markers.leftArrayBracket ? 1 : 0;
		int to = arrayContent.charAt(arrayContent.length() - 1) == markers.rightArrayBracket ? arrayContent.length() - 1 : arrayContent.length();
		arrayContent = arrayContent.substring(from, to);
		return extractArray(arrayContent, dimension);
	}

	private Object extractArray(String arrayContent, int dimension) throws SQLException {
		if (isNullValue(arrayContent))  {
			return null;
		}
		return dimension < 2 ? extractArrayFromOneDimensionalArray(arrayContent) : extractArrayFromMultiDimensionalArray(arrayContent, dimension);
	}

	@NonNull
	private Object extractArrayFromMultiDimensionalArray(String str, int dimension) throws SQLException {
		String[] s = splitToElements(str);
		int[] lengths = new int[dimension];
		lengths[0] = s.length;
		Object currentArray = Array.newInstance(columnType.getArrayBaseColumnType().getDataType().getBaseType().getType(), lengths);
		for (int i = 0; i < s.length; i++) {
			Array.set(currentArray, i, createArray(s[i], dimension - 1));
		}
		return currentArray;
	}

	private Object extractArrayFromOneDimensionalArray(String arrayContent) throws SQLException {
		FireboltDataType arrayBaseType = columnType.getArrayBaseColumnType().getDataType();
		@SuppressWarnings("java:S6204") // JDK 11 compatible
		List<String> elements = splitArrayContent(arrayContent, arrayBaseType, markers.literalQuote)
				.stream().filter(s -> s != null && !s.isEmpty()).map(x -> removeQuotesAndTransformNull(x, markers.literalQuote))
				.collect(toList());
		if (arrayBaseType == FireboltDataType.TUPLE) {
			return getArrayOfTuples(columnType, elements);
		}
		Object currentArray = Array.newInstance(arrayBaseType.getBaseType().getType(), elements.size());
		for (int i = 0; i < elements.size(); i++) {
			Array.set(currentArray, i, arrayBaseType.getBaseType().transform(elements.get(i), null));
		}
		return currentArray;
	}

	private Object[] getArrayOfTuples(ColumnType columnType, List<String> tuples) throws SQLException {
		@SuppressWarnings("java:S6204") // JDK 11 compatible
		List<FireboltDataType> types = columnType.getArrayBaseColumnType().getInnerTypes().stream()
				.map(ColumnType::getDataType).collect(toList());

		List<Object[]> list = new ArrayList<>();
		for (String tupleContent : tuples) {
			List<Object> subList = new ArrayList<>();
			List<String> tupleValues = splitArrayContent(removeParenthesis(tupleContent), FireboltDataType.TEXT, markers.tupleLiteralQuote);
			for (int j = 0; j < types.size(); j++) {
				subList.add(types.get(j).getBaseType().transform(removeQuotesAndTransformNull(tupleValues.get(j), markers.tupleLiteralQuote)));
			}
			list.add(subList.toArray());
		}
		Object[] array = new Object[list.size()];
		list.toArray(array);
		return array;
	}

	private String[] splitToElements(String value) {
		char[] chars = value.toCharArray();
		int nesting = 0;
		boolean intoString = false;
		boolean escaped = false;
		int from = 0;
		List<String> elements = new ArrayList<>();

		for (int i = 0; i < chars.length; i++) {
			char c = chars[i];
			if (c == '\\') {
				escaped = true;
				continue;
			}
			if (c == '\'' && !escaped) {
				intoString = !intoString;
			}
			if (!intoString) {
				if (c == markers.leftArrayBracket) {
					nesting++;
				} else if (c == markers.rightArrayBracket) {
					nesting--;
				} else if (c == ',') {
					if (nesting <= 0) {
						elements.add(value.substring(from, i));
						from = i + 1;
					}
				}
			}
			escaped = false;
		}
		elements.add(value.substring(from));
		return elements.toArray(new String[0]);
	}

	private String removeQuotesAndTransformNull(String s, char quote) {
		return isNullValue(s) ? "\\N" : StringUtil.strip(s, quote);
	}

	private static boolean isNullValue(String s) {
		return "NULL".equals(s);
	}

	private static String removeParenthesis(String s) {
		return s.substring(1, s.length() - 1);
	}

	private static List<String> splitArrayContent(String arrayContent, FireboltDataType baseType, char quote) {
		int index = -1;
		int subStringStart = 0;
		int parenthesisDepth = 0; // Needed for tuples
		boolean isCurrentSubstringBetweenQuotes = false;
		List<String> elements = new ArrayList<>();
		boolean escaped = false;
		while (index < arrayContent.length() - 1) {
			index++;
			char currentChar = arrayContent.charAt(index);
			if (currentChar == '\\') {
				escaped = true;
				continue;
			}
			if (currentChar == quote && !escaped) {
				isCurrentSubstringBetweenQuotes = !isCurrentSubstringBetweenQuotes;
			}
			if (!isCurrentSubstringBetweenQuotes && baseType == FireboltDataType.TUPLE) {
				if (currentChar == '(') {
					parenthesisDepth++;
				} else if (currentChar == ')') {
					parenthesisDepth--;
				}
			}
			if ((parenthesisDepth == 0 && currentChar == ',' && !isCurrentSubstringBetweenQuotes)) {
				elements.add(arrayContent.substring(subStringStart, index));
				subStringStart = index + 1;
			}
			escaped = false;
		}
		elements.add(arrayContent.substring(subStringStart));
		return elements;
	}

	public static String arrayToString(Object o) throws SQLException {
		if (o == null) {
			return null;
		}
		Object[] arr;
		if (o instanceof java.sql.Array) {
			o = ((java.sql.Array) o).getArray();
		}

		if (o.getClass().getComponentType().isPrimitive()) {
			arr = toObjectArray(o);
		} else {
			arr = (Object[]) o;
		}
		return toString(arr);
	}

	private static String toString(Object[] arr) throws FireboltException {
		if (arr == null) {
			return null;
		}
		List<String> values = new ArrayList<>();
		for (Object element : arr) {
			values.add(JavaTypeToFireboltSQLString.transformAny(element));
		}
		return "[" + String.join(",", values) + "]";
	}

	private static Object[] toObjectArray(Object array) {
		if (array == null) {
			return null;
		}
		int length = Array.getLength(array);
		Object[] ret = new Object[length];
		for (int i = 0; i < length; i++) {
			ret[i] = Array.get(array, i);
		}
		return ret;
	}

	/**
	 * Creates string representation of given byte array as a sequence of hexadecimal 2 character digits prepended
	 * by special marker {@code \x}. The same marker can be optionally used as a separator between hexadecimal digits
	 * depending on value of {@code separateEachByte}.
	 *
	 * @param bytes - the given byte array
	 * @param separateEachByte - flag that controls separator between hexadecimal digits in the resulting string
	 * @return hexadecimal representation of given array
	 */
	public static String byteArrayToHexString(@Nullable byte[] bytes, boolean separateEachByte) {
		if (bytes == null)  {
			return null;
		}
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		String separator = separateEachByte ? BYTE_ARRAY_PREFIX : "";
		return Stream.generate(buffer::get).limit(buffer.capacity()).map(i -> format("%02x", i)).collect(joining(separator, BYTE_ARRAY_PREFIX, ""));
	}

	@SuppressWarnings("java:S1168") // we have to return null here
	public static byte[] hexStringToByteArray(String str) {
		if (str == null) {
			return null;
		}
		if (!str.startsWith(BYTE_ARRAY_PREFIX)) {
			return str.getBytes(UTF_8);
		}
		char[] chars = str.substring(2).toCharArray();
		byte[] bytes = new byte[chars.length / 2];
		for (int i = 0; i < chars.length; i += 2) {
			bytes[i / 2] = (byte) ((hexDigit(chars[i]) << 4) + hexDigit(chars[i + 1]));
		}
		return bytes;
	}

	private static int hexDigit(char c) {
		int d = Character.digit(c, 16);
		if (d < 0) {
			throw new IllegalArgumentException(format("Illegal character %s in hex string", c));
		}
		return d;
	}
}
