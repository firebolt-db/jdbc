package com.firebolt.jdbc.type.array;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.column.ColumnType;
import com.firebolt.jdbc.type.FireboltDataType;
import com.firebolt.jdbc.type.JavaTypeToFireboltSQLString;
import lombok.CustomLog;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

@UtilityClass
@CustomLog
public class SqlArrayUtil {

	public static FireboltArray transformToSqlArray(String value, ColumnType columnType) throws SQLException {
		log.debug("Transformer array with value {} and type {}", value, columnType);
		int dimensions = getDimensions(value);
		Object arr = createArray(value, dimensions, columnType);
		return arr == null ? null : new FireboltArray(columnType.getArrayBaseColumnType().getDataType(), arr);
	}

	private static int getDimensions(String value) {
		char[] chars = value.toCharArray();
		int dimensions = 0;
		int dim = 0;
		boolean intoString = false;
		boolean escaped = false;

		for(char c : chars) {
			if (c == '\\') {
				escaped = true;
				continue;
			}
			if (c == '\'' && !escaped) {
				intoString = !intoString;
			}
			if (!intoString) {
				switch (c) {
					case '[':
						dim++;
						if (dim > dimensions) {
							dimensions = dim;
						}
						break;
					case ']':
						dim--;
						break;
					default: // ignore
				}
			}
			escaped = false;
		}
		return dimensions;
	}

	private static Object createArray(String arrayContent, int dimension, ColumnType columnType) throws SQLException {
		int from = arrayContent.charAt(0) == '[' ? 1 : 0;
		int to = arrayContent.charAt(arrayContent.length() - 1) == ']' ? arrayContent.length() - 1 : arrayContent.length();
		arrayContent = arrayContent.substring(from, to);
		return extractArray(arrayContent, dimension, columnType);
	}

	private static Object extractArray(String arrayContent, int dimension, ColumnType columnType) throws SQLException {
		if (isNullValue(arrayContent))  {
			return null;
		}
		return dimension < 2 ? extractArrayFromOneDimensionalArray(arrayContent, columnType) : extractArrayFromMultiDimensionalArray(arrayContent, dimension, columnType);
	}

	@NonNull
	private static Object extractArrayFromMultiDimensionalArray(String str, int dimension, ColumnType columnType) throws SQLException {
		String[] s = splitToElements(str);
		int[] lengths = new int[dimension];
		lengths[0] = s.length;
		Object currentArray = Array.newInstance(columnType.getArrayBaseColumnType().getDataType().getBaseType().getType(), lengths);
		for (int i = 0; i < s.length; i++) {
			Array.set(currentArray, i, createArray(s[i], dimension - 1, columnType));
		}
		return currentArray;
	}

	private static Object extractArrayFromOneDimensionalArray(String arrayContent, ColumnType columnType) throws SQLException {
		FireboltDataType arrayBaseType = columnType.getArrayBaseColumnType().getDataType();
		@SuppressWarnings("java:S6204") // JDK 11 compatible
		List<String> elements = splitArrayContent(arrayContent, arrayBaseType)
				.stream().filter(StringUtils::isNotEmpty).map(SqlArrayUtil::removeQuotesAndTransformNull)
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

	private static Object[] getArrayOfTuples(ColumnType columnType, List<String> tuples) throws SQLException {
		@SuppressWarnings("java:S6204") // JDK 11 compatible
		List<FireboltDataType> types = columnType.getArrayBaseColumnType().getInnerTypes().stream()
				.map(ColumnType::getDataType).collect(toList());

		List<Object[]> list = new ArrayList<>();
		for (String tupleContent : tuples) {
			List<Object> subList = new ArrayList<>();
			List<String> tupleValues = splitArrayContent(removeParenthesis(tupleContent), FireboltDataType.TEXT);
			for (int j = 0; j < types.size(); j++) {
				subList.add(types.get(j).getBaseType().transform(removeQuotesAndTransformNull(tupleValues.get(j))));
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
				switch (c) {
					case '[':
						nesting++;
						break;
					case ']':
						nesting--;
						break;
					case ',':
						if (nesting <= 0) {
							elements.add(value.substring(from, i));
							from = i + 1;
						}
						break;
					default: // ignore
				}
			}
			escaped = false;
		}
		elements.add(value.substring(from));
		return elements.toArray(new String[0]);
	}

	private static String removeQuotesAndTransformNull(String s) {
		return isNullValue(s) ? "\\N" : StringUtils.strip(s, "'");
	}

	private static boolean isNullValue(String s) {
		return "NULL".equals(s);
	}

	private static String removeParenthesis(String s) {
		return s.substring(1, s.length() - 1);
	}

	private static List<String> splitArrayContent(String arrayContent, FireboltDataType baseType) {
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
			if (currentChar == '\'' && !escaped) {
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
}
