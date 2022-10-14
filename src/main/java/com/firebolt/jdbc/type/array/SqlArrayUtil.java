package com.firebolt.jdbc.type.array;

import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.column.ColumnType;
import com.firebolt.jdbc.type.FireboltDataType;
import com.firebolt.jdbc.type.JavaTypeToFireboltSQLString;
import com.google.common.base.CharMatcher;

import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class SqlArrayUtil {

	public static FireboltArray transformToSqlArray(String value, ColumnType columnType)
			throws SQLException {
		log.debug("Transformer array with value {} and type {}", value, columnType);
		int dimensions = 0;
		for (int x = 0; x < value.length(); x++)
			if (value.charAt(x) == '[')
				dimensions++;
			else
				break;
		value = value.substring(dimensions, value.length() - dimensions);
		Object arr = createArray(value, dimensions, columnType);
		return FireboltArray.builder().array(arr).type(columnType.getArrayBaseType().getDataType()).build();
	}

	private static Object createArray(String arrayContent, int dimension, ColumnType columnType)
			throws SQLException {
		if (dimension == 1) {
			return extractArrayFromOneDimensionalArray(arrayContent, columnType);
		} else {
			return extractArrayFromMultiDimensionalArray(arrayContent, dimension, columnType);
		}
	}

	@NonNull
	private static Object extractArrayFromMultiDimensionalArray(String str, int dimension,
			ColumnType columnType) throws SQLException {
		String[] s = str.split(getArraySeparator(dimension));
		int[] lengths = new int[dimension];
		lengths[0] = s.length;
		Object currentArray = Array.newInstance(columnType.getArrayBaseType().getDataType().getBaseType().getType(), lengths);

		for (int x = 0; x < s.length; x++)
			Array.set(currentArray, x, createArray(s[x], dimension - 1, columnType));

		return currentArray;
	}

	private static Object extractArrayFromOneDimensionalArray(String arrayContent, ColumnType columnType)
			throws SQLException {
		List<String> elements = splitArrayContent(arrayContent, columnType.getArrayBaseType().getDataType()).stream()
				.filter(StringUtils::isNotEmpty).map(SqlArrayUtil::removeQuotesAndTransformNull)
				.collect(Collectors.toList());
		FireboltDataType arrayBaseType = columnType.getArrayBaseType().getDataType();
		if (arrayBaseType != FireboltDataType.TUPLE) {
			Object currentArray = Array.newInstance(arrayBaseType.getBaseType().getType(), elements.size());
			for (int i = 0; i < elements.size(); i++)
				Array.set(currentArray, i, arrayBaseType.getBaseType().transform(elements.get(i), null));
			return currentArray;
		} else {
			return getArrayOfTuples(columnType, elements);
		}
	}

	private static Object[] getArrayOfTuples(ColumnType columnType, List<String> tuples)
			throws SQLException {
		List<FireboltDataType> types = columnType.getArrayBaseType().getInnerTypes().stream().map(ColumnType::getDataType)
				.collect(Collectors.toList());

		List<Object[]> list = new ArrayList<>();
		for (String tupleContent : tuples) {
			List<Object> subList = new ArrayList<>();
			List<String> tupleValues = splitArrayContent(removeParenthesis(tupleContent), FireboltDataType.STRING);
			for (int j = 0; j < types.size(); j++) {
				subList.add(types.get(j).getBaseType().transform(removeQuotesAndTransformNull(tupleValues.get(j))));
			}
			list.add(subList.toArray());
		}
		Object[] array = new Object[list.size()];
		list.toArray(array);
		return array;
	}

	private static String getArraySeparator(int dimension) {
		StringBuilder stringBuilder = new StringBuilder(",");
		for (int x = 1; x < dimension; x++) {
			stringBuilder.insert(0, ']');
			stringBuilder.append("\\[");
		}
		return stringBuilder.toString();
	}

	private static String removeQuotesAndTransformNull(String s) {
		if (StringUtils.equals(s, "NULL")) {
			return "\\N";
		}
		return CharMatcher.is('\'').trimFrom(s);
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
		while (index < arrayContent.length() - 1) {
			index++;
			char currentChar = arrayContent.charAt(index);
			if (currentChar == 39) {
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
		}
		elements.add(arrayContent.substring(subStringStart));
		return elements;
	}

	public static String arrayToString(Object o) throws SQLException {
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
		List<String> values = new ArrayList<>();
		for (Object element : arr) {
			values.add(JavaTypeToFireboltSQLString.transformAny(element));
		}
		return "[" + String.join(",", values) + "]";
	}

	private static Object[] toObjectArray(Object array) {
		int length = Array.getLength(array);
		Object[] ret = new Object[length];
		for (int i = 0; i < length; i++)
			ret[i] = Array.get(array, i);
		return ret;
	}
}
