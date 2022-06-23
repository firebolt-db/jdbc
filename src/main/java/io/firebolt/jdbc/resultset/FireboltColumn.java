package io.firebolt.jdbc.resultset;

import io.firebolt.jdbc.resultset.type.FireboltDataType;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Optional;

import static io.firebolt.jdbc.resultset.type.FireboltDataType.*;

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
  private final int arrayDepth;
  private final int precision;
  private final int scale;
  private Pair<FireboltColumn, FireboltColumn> columnsTuple;

  public static FireboltColumn of(String columnType, String columnName) {
    log.debug("Creating column info for column: {} of type: {}", columnName, columnType);
    String typeInUpperCase = StringUtils.upperCase(columnType);
    int currentIndex = 0;
    int arrayDepth = 0;
    boolean isNullable = false;
    FireboltDataType arrayType = null;
    Pair<FireboltColumn, FireboltColumn> tuple = null;
    Optional<Pair<Optional<Integer>, Optional<Integer>>> scaleAndPrecisionPair;
    FireboltDataType fireboltType;
    if (typeInUpperCase.startsWith(FireboltDataType.TUPLE.getInternalName().toUpperCase())) {
      tuple = getColumnsTuple(typeInUpperCase, columnName);
    }

    while (typeInUpperCase.startsWith(
        FireboltDataType.ARRAY.getInternalName().toUpperCase(), currentIndex)) {
      arrayDepth++;
      currentIndex += FireboltDataType.ARRAY.getInternalName().length() + 1;
    }

    if (typeInUpperCase.startsWith(NULLABLE_TYPE, currentIndex)) {
      isNullable = true;
      currentIndex += NULLABLE_TYPE.length() + 1;
    }
    int typeEndIndex = getTypeEndPosition(typeInUpperCase, currentIndex);
    FireboltDataType dataType =
        FireboltDataType.ofType(typeInUpperCase.substring(currentIndex, typeEndIndex));
    if (arrayDepth > 0) {
      arrayType = dataType;
      if (arrayType == TUPLE) {
        String tmp = columnType.substring(currentIndex, columnType.length() - arrayDepth);
        tuple = getColumnsTuple(tmp.toUpperCase(), columnName);
      }
      fireboltType = FireboltDataType.ARRAY;
    } else {
      fireboltType = dataType;
    }

    if (!reachedEndOfTypeName(typeEndIndex, typeInUpperCase.length())
        || typeInUpperCase.startsWith("(", typeEndIndex)) {
      String[] arguments = splitArguments(typeInUpperCase, typeEndIndex);
      scaleAndPrecisionPair = Optional.of(getsCaleAndPrecision(arguments, dataType));
    } else {
      scaleAndPrecisionPair = Optional.empty();
    }

    return FireboltColumn.builder()
        .columnName(columnName)
        .columnType(typeInUpperCase)
        .scale(
            scaleAndPrecisionPair
                .map(Pair::getLeft)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .orElse(dataType.getDefaultScale()))
        .precision(
            scaleAndPrecisionPair
                .map(Pair::getRight)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .orElse(dataType.getDefaultPrecision()))
        .arrayBaseDataType(arrayType)
        .dataType(fireboltType)
        .nullable(isNullable)
        .arrayDepth(arrayDepth)
        .columnsTuple(tuple)
        .build();
  }
  public static FireboltColumn of(String columnType) {
    return of(columnType, null);
  }

  private static Pair<FireboltColumn, FireboltColumn> getColumnsTuple(
      String columnType, String columnName) {
    String[] types = getTupleTypes(columnType);
    FireboltColumn leftColumnType = null;
    FireboltColumn rightColumnType = null;

    if (types.length > 0) {
      leftColumnType = FireboltColumn.of(types[0].trim(), columnName);
      if (types.length > 1) {
        rightColumnType = FireboltColumn.of(types[1].trim(), columnName);
      }
    }
    return new ImmutablePair<>(leftColumnType, rightColumnType);
  }

  @NonNull
  private static String[] getTupleTypes(String columnType) {
    String types =
        RegExUtils.replaceFirst(
            columnType, FireboltDataType.TUPLE.getInternalName().toUpperCase() + "\\(", "");
    types = StringUtils.substring(types, 0, types.length() - 1);
    return types.split(",(?![^()]*\\))"); // Regex to split on comma and ignoring comma that are between parenthesis
  }

  private static boolean reachedEndOfTypeName(int typeNameEndIndex, int type) {
    return typeNameEndIndex == type;
  }

  private static int getTypeEndPosition(String type, int currentIndex) {
    int typeNameEndIndex =
        type.indexOf("(", currentIndex) < 0
            ? type.indexOf(")", currentIndex)
            : type.indexOf("(", currentIndex);

    return typeNameEndIndex < 0 ? type.length() : typeNameEndIndex;
  }

  private static Pair<Optional<Integer>, Optional<Integer>> getsCaleAndPrecision(
      String[] arguments, FireboltDataType dataType) {
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
        if (reachedEndOfTypeName(arguments.length, 2)) {
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
    return StringUtils.substring(args, args.indexOf("(", index) + 1, args.indexOf(")", index))
        .split("\\s*,\\s*");
  }

  public String getCompactTypeName() {
    if (this.isArray()) {
      return getArrayCompactTypeName();
    } else if (this.isTuple()) {
      return getTupleCompactTypeName(this.columnsTuple);
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
      type.append(this.getTupleCompactTypeName(this.getColumnsTuple()));
    }
    for (int i = 0; i < arrayDepth; i++) {
      type.append(")");
    }
    return type.toString();
  }

  private String getTupleCompactTypeName(Pair<FireboltColumn, FireboltColumn> columnsTuple) {
    if (columnsTuple.getRight() != null) {
      return String.format(
          "%s(%s, %s)",
          TUPLE.getDisplayName(),
          columnsTuple.getLeft().getCompactTypeName(),
          columnsTuple.getRight().getCompactTypeName());
    } else if (columnsTuple.getLeft() != null) {
      return String.format(
          "%s(%s)", TUPLE.getDisplayName(), columnsTuple.getLeft().getCompactTypeName());
    } else {
      return String.format("%s", TUPLE.getDisplayName());
    }
  }

  private Optional<String> getTypeArguments(String type) {
    return Optional.ofNullable(getTypeWithoutNullableKeyword(type))
        .filter(t -> t.contains("("))
        .map(t -> t.substring(t.indexOf("(")));
  }

  private String getTypeWithoutNullableKeyword(String columnType) {
    return Optional.ofNullable(columnType)
        .filter(t -> this.nullable)
        .map(type -> StringUtils.remove(type, NULLABLE_TYPE + "("))
        .map(type -> StringUtils.removeEnd(type, ")"))
        .orElse(columnType);
  }

  private boolean isArray() {
    return this.dataType.equals(ARRAY);
  }

  private boolean isTuple() {
    return this.dataType.equals(TUPLE);
  }
}
