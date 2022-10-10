package com.firebolt.jdbc.resultset;

import static com.firebolt.jdbc.type.FireboltDataType.*;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.firebolt.jdbc.type.FireboltDataType;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Builder
@Getter
@EqualsAndHashCode
@Slf4j
public final class FireboltColumn {
	private final String columnType;
	private final String columnName;
	private final FireboltDataType dataType;
	private final boolean nullable;
	private final FireboltDataType arrayBaseDataType;
	private final List<FireboltColumn> tupleBaseDataTypes;
	private final int arrayDepth;
	private final int precision;
	private final int scale;
	private final TimeZone timeZone;
	private static final Set<String> TIMEZONES = Arrays.stream(TimeZone.getAvailableIDs())
			.collect(Collectors.toCollection(HashSet::new));

	public static FireboltColumn of(String columnType, String columnName) {
		log.debug("Creating column info for column: {} of type: {}", columnName, columnType);
		String typeInUpperCase = StringUtils.upperCase(columnType);
		int currentIndex = 0;
		int arrayDepth = 0;
		boolean isNullable = false;
		FireboltDataType arrayType = null;
		List<FireboltColumn> tupleDataTypes = null;
		TimeZone timeZone = null;
		Optional<Pair<Optional<Integer>, Optional<Integer>>> scaleAndPrecisionPair;
		FireboltDataType fireboltType;
		if (typeInUpperCase.startsWith(FireboltDataType.TUPLE.getInternalName().toUpperCase())) {
			tupleDataTypes = getTupleBaseDataTypes(typeInUpperCase, columnName);
		}

		while (typeInUpperCase.startsWith(FireboltDataType.ARRAY.getInternalName().toUpperCase(), currentIndex)) {
			arrayDepth++;
			currentIndex += FireboltDataType.ARRAY.getInternalName().length() + 1;
		}

		if (typeInUpperCase.startsWith(NULLABLE_TYPE, currentIndex)) {
			isNullable = true;
			currentIndex += NULLABLE_TYPE.length() + 1;
		}
		int typeEndIndex = getTypeEndPosition(typeInUpperCase, currentIndex);
		FireboltDataType dataType = FireboltDataType.ofType(typeInUpperCase.substring(currentIndex, typeEndIndex));
		if (arrayDepth > 0) {
			arrayType = dataType;
			if (arrayType == TUPLE) {
				String tmp = columnType.substring(currentIndex, columnType.length() - arrayDepth);
				tupleDataTypes = getTupleBaseDataTypes(tmp.toUpperCase(), columnName);
			}
			fireboltType = FireboltDataType.ARRAY;
		} else {
			fireboltType = dataType;
		}
		String[] arguments = null;
		if (!reachedEndOfTypeName(typeEndIndex, typeInUpperCase) || typeInUpperCase.startsWith("(", typeEndIndex)) {
			arguments = splitArguments(typeInUpperCase, typeEndIndex);
			scaleAndPrecisionPair = Optional.of(getsCaleAndPrecision(arguments, dataType));
		} else {
			scaleAndPrecisionPair = Optional.empty();
		}
		if (dataType.isTime() && arguments != null && arguments.length != 0) {
			timeZone = getTimeZoneFromArguments(arguments);
		}

		return FireboltColumn.builder().columnName(columnName).columnType(typeInUpperCase)
				.scale(scaleAndPrecisionPair.map(Pair::getLeft).filter(Optional::isPresent).map(Optional::get)
						.orElse(dataType.getDefaultScale()))
				.precision(scaleAndPrecisionPair.map(Pair::getRight).filter(Optional::isPresent).map(Optional::get)
						.orElse(dataType.getDefaultPrecision()))
				.timeZone(timeZone).arrayBaseDataType(arrayType).dataType(fireboltType).nullable(isNullable)
				.arrayDepth(arrayDepth).tupleBaseDataTypes(tupleDataTypes).build();
	}

	private static TimeZone getTimeZoneFromArguments(String[] arguments) {
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

	public static FireboltColumn of(String columnType) {
		return of(columnType, null);
	}

	private static List<FireboltColumn> getTupleBaseDataTypes(String columnType, String columnName) {
		return Arrays.stream(getTupleTypes(columnType)).map(String::trim)
				.map(type -> FireboltColumn.of(type, columnName)).collect(Collectors.toList());
	}

	@NonNull
	private static String[] getTupleTypes(String columnType) {
		String types = RegExUtils.replaceFirst(columnType,
				FireboltDataType.TUPLE.getInternalName().toUpperCase() + "\\(", "");
		types = StringUtils.substring(types, 0, types.length() - 1);
		return types.split(",(?![^()]*\\))"); // Regex to split on comma and ignoring comma that are between
												// parenthesis
	}

	private static boolean reachedEndOfTypeName(int typeNameEndIndex, String type) {
		return typeNameEndIndex == type.length() || type.indexOf("(", typeNameEndIndex) < 0
				|| type.indexOf(")", typeNameEndIndex) < 0;
	}

	private static int getTypeEndPosition(String type, int currentIndex) {
		int typeNameEndIndex = type.indexOf("(", currentIndex) < 0 ? type.indexOf(")", currentIndex)
				: type.indexOf("(", currentIndex);

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
				precision = dataType.getDefaultPrecision() + scale;
			}
			break;
		case DECIMAL:
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

	public String getCompactTypeName() {
		if (this.isArray()) {
			return getArrayCompactTypeName();
		} else if (this.isTuple()) {
			return getTupleCompactTypeName(this.tupleBaseDataTypes);
		} else {
			Optional<String> params = getTypeArguments(columnType);
			return dataType.getDisplayName() + params.orElse("");
		}
	}

	private String getArrayCompactTypeName() {
		StringBuilder type = new StringBuilder();
		for (int i = 0; i < arrayDepth; i++) {
			type.append(ARRAY.getDisplayName());
			type.append("(");
		}
		if (this.getArrayBaseDataType() != TUPLE) {
			type.append(this.getArrayBaseDataType().getDisplayName());
		} else {
			type.append(this.getTupleCompactTypeName(this.getTupleBaseDataTypes()));
		}
		for (int i = 0; i < arrayDepth; i++) {
			type.append(")");
		}
		return type.toString();
	}

	private String getTupleCompactTypeName(List<FireboltColumn> columnsTuple) {

		return columnsTuple.stream().map(FireboltColumn::getCompactTypeName)
				.collect(Collectors.joining(", ", TUPLE.getDisplayName() + "(", ")"));
	}

	private Optional<String> getTypeArguments(String type) {
		return Optional.ofNullable(getTypeWithoutNullableKeyword(type)).filter(t -> t.contains("("))
				.map(t -> t.substring(t.indexOf("(")));
	}

	private String getTypeWithoutNullableKeyword(String columnType) {
		return Optional.ofNullable(columnType).filter(t -> this.nullable)
				.map(type -> StringUtils.remove(type, NULLABLE_TYPE + "("))
				.map(type -> StringUtils.removeEnd(type, ")")).orElse(columnType);
	}

	private boolean isArray() {
		return this.dataType.equals(ARRAY);
	}

	private boolean isTuple() {
		return this.dataType.equals(TUPLE);
	}
}
