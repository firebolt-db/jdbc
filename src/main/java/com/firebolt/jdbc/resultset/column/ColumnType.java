package com.firebolt.jdbc.resultset.column;

import static com.firebolt.jdbc.type.FireboltDataType.*;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.firebolt.jdbc.type.FireboltDataType;

import lombok.*;

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
	private static final String NULLABLE_TYPE = "NULLABLE";
	private static final Set<String> TIMEZONES = Arrays.stream(TimeZone.getAvailableIDs())
			.collect(Collectors.toCollection(HashSet::new));
	@EqualsAndHashCode.Exclude
	String name;
	FireboltDataType dataType;
	boolean nullable;
	Integer precision;
	Integer scale;
	TimeZone timeZone;
	List<ColumnType> innerTypes;

	public static ColumnType of(String columnType) {
		List<ColumnType> innerDataTypes = null;
		TimeZone timeZone = null;
		Optional<Pair<Optional<Integer>, Optional<Integer>>> scaleAndPrecisionPair;
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
		if (dataType.isTime() && ArrayUtils.isNotEmpty(arguments)) {
			timeZone = getTimeZoneFromArguments(arguments);
		}
		return builder().name(columnTypeWrapper.getTypeInUpperCase()).nullable(isNullable).dataType(fireboltType)
				.scale(scaleAndPrecisionPair.map(Pair::getLeft).filter(Optional::isPresent).map(Optional::get)
						.orElse(dataType.getMaxScale()))
				.precision(scaleAndPrecisionPair.map(Pair::getRight).filter(Optional::isPresent).map(Optional::get)
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
			if(typeWithoutNullKeyword.startsWith(type.toUpperCase())) {
				typeWithoutNullKeyword = RegExUtils.replaceFirst(typeWithoutNullKeyword, type.toUpperCase() + "\\(", "");
				typeWithoutNullKeyword = StringUtils.substring(typeWithoutNullKeyword, 0, typeWithoutNullKeyword.length() - 1);
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

	private static Pair<Optional<Integer>, Optional<Integer>> getsCaleAndPrecision(String[] arguments,
			FireboltDataType dataType) {
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
		return new ImmutablePair<>(Optional.ofNullable(scale), Optional.ofNullable(precision));
	}

	private static String[] splitArguments(String args, int index) {
		return StringUtils.substring(args, args.indexOf("(", index) + 1, args.indexOf(")", index)).split("\\s*,\\s*");
	}

	private static TimeZone getTimeZoneFromArguments(@NonNull String[] arguments) {
		String timeZoneArgument = null;
		TimeZone timeZone = null;
		if (arguments.length > 1) {
			timeZoneArgument = arguments[1];
		} else if (arguments.length == 1 && !StringUtils.isNumeric(arguments[0])) {
			timeZoneArgument = arguments[0];
		}
		if (timeZoneArgument != null) {
			String id = timeZoneArgument.replace("\\'", "");
			if (TIMEZONES.contains(id)) {
				timeZone = TimeZone.getTimeZone(timeZoneArgument.replace("\\'", ""));
			} else {
				log.warn("Could not use the timezone returned by the server with the id {} as it is not supported.",
						id);
			}
		}
		return timeZone;
	}

	public String getCompactTypeName() {
		if (this.isArray()) {
			return getArrayCompactTypeName();
		} else if (this.isType()) {
			return getTupleCompactTypeName(this.innerTypes);
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
			compactType.append(ARRAY.getDisplayName() + "(");
			columnType = columnType.getInnerTypes().isEmpty() ? null : columnType.getInnerTypes().get(0);
		}
		compactType.append(this.getArrayBaseColumnType().getCompactTypeName());
		for (int i = 0; i < depth; i++) {
			compactType.append(")");
		}
		return compactType.toString();
	}

	private String getTupleCompactTypeName(List<ColumnType> columnsTuple) {
		return columnsTuple.stream().map(ColumnType::getCompactTypeName)
				.collect(Collectors.joining(", ", TUPLE.getDisplayName() + "(", ")"));
	}

	private boolean isArray() {
		return dataType.equals(ARRAY);
	}

	private boolean isType() {
		return dataType.equals(TUPLE);
	}

	public ColumnType getArrayBaseColumnType() {
		if (innerTypes == null || innerTypes.isEmpty()) {
			return null;
		}
		ColumnType currentInnerType = this.innerTypes.get(0);
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
			String typeInUpperCase = StringUtils.upperCase(type);
			String typeWithoutNullableKeyword = typeInUpperCase;
			if (typeInUpperCase.startsWith(NULLABLE_TYPE)) {
				isNullable = true;
				typeWithoutNullableKeyword = typeInUpperCase.substring(NULLABLE_TYPE.length() + 1,
						typeInUpperCase.length() - 1);
			} else if (typeInUpperCase.endsWith(NOT_NULLABLE_TYPE)) {
				typeWithoutNullableKeyword = StringUtils.removeEnd(typeInUpperCase, " " + NOT_NULLABLE_TYPE);
			} else if (typeInUpperCase.endsWith(NULL_TYPE)) {
				isNullable = true;
				typeWithoutNullableKeyword = StringUtils.removeEnd(typeInUpperCase, " " + NULL_TYPE);
			}
			return new ColumnTypeWrapper(type, typeInUpperCase, typeWithoutNullableKeyword, isNullable);

		}
	}

}
