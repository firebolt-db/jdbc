package com.firebolt.jdbc.resultset.column;

import static com.firebolt.jdbc.type.FireboltDataType.*;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.firebolt.jdbc.type.FireboltDataType;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
@Value
public class ColumnType {
	String typeName;
	FireboltDataType dataType;
	boolean nullable;
	int arrayDepth;
	int precision;
	int scale;
	TimeZone timeZone;
	List<ColumnType> innerTypes;

	private static final Set<String> TIMEZONES = Arrays.stream(TimeZone.getAvailableIDs())
			.collect(Collectors.toCollection(HashSet::new));

	public static ColumnType of(String columnType) {
		String typeInUpperCase = StringUtils.upperCase(columnType);
		int currentIndex = 0;
		int arrayDepth = 0;
		boolean isNullable = false;
		List<ColumnType> tupleDataTypes = null;
		TimeZone timeZone = null;
		Optional<Pair<Optional<Integer>, Optional<Integer>>> scaleAndPrecisionPair;
		FireboltDataType fireboltType;
		String typeWithoutNullableKeyword = typeInUpperCase;

		if (typeInUpperCase.startsWith(NULLABLE_TYPE, currentIndex)) {
			isNullable = true;
			typeWithoutNullableKeyword = typeInUpperCase.substring(NULLABLE_TYPE.length() + 1, typeInUpperCase.length() - 1);
		}

		if (typeWithoutNullableKeyword.startsWith(FireboltDataType.TUPLE.getInternalName().toUpperCase())
				|| typeWithoutNullableKeyword.startsWith(ARRAY.getInternalName().toUpperCase())) {
			tupleDataTypes = getInnerTypes(typeWithoutNullableKeyword);
		}

		int typeEndIndex = getTypeEndPosition(typeWithoutNullableKeyword);
		FireboltDataType dataType = ofType(typeWithoutNullableKeyword.substring(0, typeEndIndex));
		fireboltType = dataType;
		String[] arguments = null;
		if (!reachedEndOfTypeName(typeEndIndex, typeWithoutNullableKeyword)
				|| typeWithoutNullableKeyword.startsWith("(", typeEndIndex)) {
			arguments = splitArguments(typeWithoutNullableKeyword, typeEndIndex);
			scaleAndPrecisionPair = Optional.of(getsCaleAndPrecision(arguments, dataType));
		} else {
			scaleAndPrecisionPair = Optional.empty();
		}
		if (dataType.isTime() && arguments != null && arguments.length != 0) {
			timeZone = getTimeZoneFromArguments(arguments);
		}

		return builder().typeName(typeInUpperCase).nullable(isNullable).dataType(fireboltType)
				.scale(scaleAndPrecisionPair.map(Pair::getLeft).filter(Optional::isPresent).map(Optional::get)
						.orElse(dataType.getDefaultScale()))
				.precision(scaleAndPrecisionPair.map(Pair::getRight).filter(Optional::isPresent).map(Optional::get)
						.orElse(dataType.getDefaultPrecision()))
				.timeZone(timeZone).arrayDepth(arrayDepth)
				.innerTypes(Optional.ofNullable(tupleDataTypes).orElse(new ArrayList<>())).build();
	}

	private boolean isArray() {
		return dataType.equals(ARRAY);
	}

	private boolean isTuple() {
		return dataType.equals(TUPLE);
	}

	private String getArrayCompactTypeName() {
		StringBuilder compactType = new StringBuilder();
		int depth = 0;
		ColumnType columnType = this;
		while (columnType != null && columnType.getDataType() == ARRAY) {
			depth++;
			compactType.append("ARRAY(");
			columnType = columnType.getInnerTypes().isEmpty() ? null : columnType.getInnerTypes().get(0);
		}
		compactType.append(this.getArrayBaseType().getCompactTypeName());
		for (int i = 0; i < depth; i++) {
			compactType.append(")");
		}
		return compactType.toString();
	}

	private String getTupleCompactTypeName(List<ColumnType> columnsTuple) {
		return columnsTuple.stream().map(ColumnType::getCompactTypeName)
				.collect(Collectors.joining(", ", TUPLE.getDisplayName() + "(", ")"));
	}

	private Optional<String> getTypeArguments(String type) {
		return Optional.ofNullable(getTypeWithoutNullableKeyword(type)).filter(t -> t.contains("("))
				.map(t -> t.substring(t.indexOf("(")));
	}

	public String getCompactTypeName() {
		if (this.isArray()) {
			return getArrayCompactTypeName();
		} else if (this.isTuple()) {
			return getTupleCompactTypeName(this.innerTypes);
		} else {
			Optional<String> params = getTypeArguments(typeName);
			return dataType.getDisplayName() + params.orElse("");
		}
	}

	private String getTypeWithoutNullableKeyword(String columnType) {
		return Optional.ofNullable(columnType).filter(t -> nullable)
				.map(type -> StringUtils.remove(type, NULLABLE_TYPE + "("))
				.map(type -> StringUtils.removeEnd(type, ")")).orElse(columnType);
	}

	private static List<ColumnType> getInnerTypes(String columnType) {
		if (columnType.startsWith(FireboltDataType.TUPLE.getInternalName().toUpperCase())) {
			return Arrays.stream(getTupleTypes(columnType)).map(String::trim).map(ColumnType::of)
					.collect(Collectors.toList());
		} else {
			return Arrays.stream(getArrayType(columnType)).map(String::trim).map(ColumnType::of)
					.collect(Collectors.toList());
		}
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

	public ColumnType getArrayBaseType() {
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

	@NonNull
	private static String[] getTupleTypes(String columnType) {
		String types = RegExUtils.replaceFirst(columnType,
				FireboltDataType.TUPLE.getInternalName().toUpperCase() + "\\(", "");
		types = StringUtils.substring(types, 0, types.length() - 1);
		return types.split(",(?![^()]*\\))"); // Regex to split on comma and ignoring comma that are between
		// parenthesis
	}

	@NonNull
	private static String[] getArrayType(String columnType) {
		String types = RegExUtils.replaceFirst(columnType, ARRAY.getInternalName().toUpperCase() + "\\(", "");
		types = StringUtils.substring(types, 0, types.length() - 1);
		return new String[] { types };
	}

}
