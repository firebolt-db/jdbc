package com.firebolt.jdbc.resultset.column;

import com.firebolt.jdbc.type.FireboltDataType;
import lombok.Builder;
import lombok.CustomLog;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.firebolt.jdbc.type.FireboltDataType.ARRAY;
import static com.firebolt.jdbc.type.FireboltDataType.TUPLE;
import static com.firebolt.jdbc.type.FireboltDataType.ofType;

/**
 * This class represents a Column type returned by the server
 */
@CustomLog
@Builder
@Value
@EqualsAndHashCode
public class ColumnType {
	private static final String NOT_NULLABLE_TYPE = "NOT NULL";
	private static final String NULL_TYPE = "NULL";
	private static final String NOT_NULLABLE_TYPE_SUFFIX = " " + NOT_NULLABLE_TYPE;
	private static final String NULL_TYPE_SUFFIX = " " + NULL_TYPE;
	private static final String NULLABLE_TYPE = "NULLABLE";
	private static final Set<String> TIMEZONES = Arrays.stream(TimeZone.getAvailableIDs())
			.collect(Collectors.toCollection(HashSet::new));
	private static final Pattern COMMA_WITH_SPACES = Pattern.compile("\\s*,\\s*");
	@EqualsAndHashCode.Exclude
	String name;
	FireboltDataType dataType;
	boolean nullable;
	int precision;
	int scale;
	TimeZone timeZone;
	List<ColumnType> innerTypes;

	public static ColumnType of(String columnType) {
		List<ColumnType> innerDataTypes = null;
		TimeZone timeZone = null;
		Optional<Entry<Optional<Integer>, Optional<Integer>>> scaleAndPrecisionPair;
		FireboltDataType fireboltType;
		ColumnTypeWrapper columnTypeWrapper = ColumnTypeWrapper.of(columnType);
		String typeWithoutNullKeyword = columnTypeWrapper.getTypeWithoutNullKeyword();
		boolean isNullable = columnTypeWrapper.isNullable();

		if (isType(FireboltDataType.ARRAY, typeWithoutNullKeyword) ) {
			innerDataTypes = getCollectionSubType(FireboltDataType.ARRAY, typeWithoutNullKeyword);
		} else if (isType(FireboltDataType.TUPLE, typeWithoutNullKeyword)) {
			innerDataTypes = getCollectionSubType(FireboltDataType.TUPLE, typeWithoutNullKeyword);
		}

		int typeEndIndex = getTypeEndPosition(typeWithoutNullKeyword);
		FireboltDataType dataType = ofType(typeWithoutNullKeyword.substring(0, typeEndIndex));
		fireboltType = dataType;
		String[] arguments = null;
		if (!reachedEndOfTypeName(typeEndIndex, typeWithoutNullKeyword)
				|| typeWithoutNullKeyword.startsWith("(", typeEndIndex)) {
			arguments = splitArguments(typeWithoutNullKeyword, typeEndIndex);
			scaleAndPrecisionPair = Optional.of(getsCaleAndPrecision(arguments, dataType));
		} else {
			scaleAndPrecisionPair = Optional.empty();
		}
		if (dataType.isTime() && arguments != null && arguments.length > 0) {
			timeZone = getTimeZoneFromArguments(arguments);
		}
		return builder().name(columnTypeWrapper.getTypeInUpperCase()).nullable(isNullable).dataType(fireboltType)
				.scale(scaleAndPrecisionPair.map(Entry::getKey).filter(Optional::isPresent).map(Optional::get)
						.orElse(dataType.getMaxScale()))
				.precision(scaleAndPrecisionPair.map(Entry::getValue).filter(Optional::isPresent).map(Optional::get)
						.orElse(dataType.getPrecision()))
				.timeZone(timeZone).innerTypes(Optional.ofNullable(innerDataTypes).orElse(new ArrayList<>())).build();
	}

	private static boolean isType(FireboltDataType fireboltDataType, String typeWithoutNullKeyword) {
		for (String type : fireboltDataType.getAliases()) {
			if (typeWithoutNullKeyword.startsWith(type.toUpperCase())) {
				return true;
			}
		}
		return false;
	}

	private static List<ColumnType> getCollectionSubType(FireboltDataType fireboltDataType, String typeWithoutNullKeyword) {
		String[] types;
		for (String type: fireboltDataType.getAliases()) {
			String typeUpperCase = type.toUpperCase();
			if (typeWithoutNullKeyword.startsWith(typeUpperCase)) {
				typeWithoutNullKeyword = typeWithoutNullKeyword.substring((typeUpperCase + "\\(").length() - 1, typeWithoutNullKeyword.length() - 1);
				break;
			}
		}

		if (fireboltDataType.equals(TUPLE)) {
			types = typeWithoutNullKeyword.split(",(?![^()]*\\))"); // Regex to split on comma and ignoring comma that are between
			// parenthesis
		} else {
			types = new String[] {typeWithoutNullKeyword};
		}

		return Arrays.stream(types)
				.map(String::trim).map(ColumnType::of)
				.collect(Collectors.toList());
	}

	private static boolean reachedEndOfTypeName(int typeNameEndIndex, String type) {
		return typeNameEndIndex == type.length() || type.indexOf("(", typeNameEndIndex) < 0
				|| type.indexOf(")", typeNameEndIndex) < 0;
	}

	private static int getTypeEndPosition(String type) {
		int typeNameEndIndex = !type.contains("(") ? type.indexOf(")") : type.indexOf("(");
		return typeNameEndIndex < 0 ? type.length() : typeNameEndIndex;
	}

	private static Entry<Optional<Integer>, Optional<Integer>> getsCaleAndPrecision(String[] arguments, FireboltDataType dataType) {
		Integer scale = null;
		Integer precision = null;
		switch (dataType) {
		case DATE_TIME_64:
			if (arguments.length >= 1) {
				scale = Integer.parseInt(arguments[0]);
				precision = dataType.getPrecision() + scale;
			}
			break;
		case NUMERIC:
			if (arguments.length == 2) {
				precision = Integer.parseInt(arguments[0]);
				scale = Integer.parseInt(arguments[1]);
			}
			break;
		default:
			break;
		}
		return Map.entry(Optional.ofNullable(scale), Optional.ofNullable(precision));
	}

	private static String[] splitArguments(String args, int index) {
		return COMMA_WITH_SPACES.split(args.substring(args.indexOf("(", index) + 1, args.indexOf(")", index)));
	}

	private static TimeZone getTimeZoneFromArguments(@NonNull String[] arguments) {
		String timeZoneArgument = null;
		TimeZone timeZone = null;
		if (arguments.length > 1) {
			timeZoneArgument = arguments[1];
		} else if (arguments.length == 1 && arguments[0].chars().anyMatch(c -> !Character.isDigit(c))) {
			timeZoneArgument = arguments[0];
		}
		if (timeZoneArgument != null) {
			String id = timeZoneArgument.replace("\\'", "");
			if (TIMEZONES.contains(id)) {
				timeZone = TimeZone.getTimeZone(timeZoneArgument.replace("\\'", ""));
			} else {
				log.warn("Could not use the timezone returned by the server with the id {} as it is not supported.", id);
			}
		}
		return timeZone;
	}

	public String getCompactTypeName() {
		if (isArray()) {
			return getArrayCompactTypeName();
		} else if (isTuple()) {
			return getTupleCompactTypeName(innerTypes);
		} else {
			return dataType.getDisplayName();
		}
	}

	private String getArrayCompactTypeName() {
		StringBuilder compactType = new StringBuilder();
		int depth = 0;
		ColumnType columnType = this;
		while (columnType != null && columnType.getDataType() == ARRAY) {
			depth++;
			compactType.append(ARRAY.getDisplayName()).append("(");
			columnType = columnType.getInnerTypes().isEmpty() ? null : columnType.getInnerTypes().get(0);
		}
		compactType.append(getArrayBaseColumnType().getCompactTypeName());
		compactType.append(")".repeat(depth));
		return compactType.toString();
	}

	private String getTupleCompactTypeName(List<ColumnType> columnsTuple) {
		return columnsTuple.stream().map(ColumnType::getCompactTypeName)
				.collect(Collectors.joining(", ", TUPLE.getDisplayName() + "(", ")"));
	}

	private boolean isArray() {
		return dataType.equals(ARRAY);
	}

	private boolean isTuple() {
		return dataType.equals(TUPLE);
	}

	public ColumnType getArrayBaseColumnType() {
		if (innerTypes == null || innerTypes.isEmpty()) {
			return null;
		}
		ColumnType currentInnerType = innerTypes.get(0);
		while (currentInnerType.getInnerTypes() != null && !currentInnerType.getInnerTypes().isEmpty()
				&& currentInnerType.getDataType() != TUPLE) {
			currentInnerType = currentInnerType.getInnerTypes().get(0);
		}
		return currentInnerType;
	}

	@Getter
	@Value
	private static class ColumnTypeWrapper {
		String type;
		String typeInUpperCase;
		String typeWithoutNullKeyword;
		boolean nullable;

		public static ColumnTypeWrapper of(String type) {
			boolean isNullable = false;
			String typeInUpperCase = type == null ? null : type.toUpperCase();
			String typeWithoutNullableKeyword = typeInUpperCase;
			if (typeInUpperCase.startsWith(NULLABLE_TYPE)) {
				isNullable = true;
				typeWithoutNullableKeyword = typeInUpperCase.substring(NULLABLE_TYPE.length() + 1, typeInUpperCase.length() - 1);
			} else if (typeInUpperCase.endsWith(NOT_NULLABLE_TYPE_SUFFIX)) {
				typeWithoutNullableKeyword = typeInUpperCase.substring(0, typeInUpperCase.length() - NOT_NULLABLE_TYPE_SUFFIX.length());
			} else if (typeInUpperCase.endsWith(NULL_TYPE_SUFFIX)) {
				isNullable = true;
				typeWithoutNullableKeyword = typeInUpperCase.substring(0, typeInUpperCase.length() - NULL_TYPE_SUFFIX.length());
			} else if (typeInUpperCase.endsWith(NULL_TYPE)) {
				isNullable = true;
			}
			return new ColumnTypeWrapper(type, typeInUpperCase, typeWithoutNullableKeyword, isNullable);
		}
	}
}
