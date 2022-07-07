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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
  private final List<FireboltColumn> tupleBaseDateTypes;
  private final int arrayDepth;
  private final int precision;
  private final int scale;

  public static FireboltColumn of(String columnType, String columnName) {
    log.debug("Creating column info for column: {} of type: {}", columnName, columnType);
    String typeInUpperCase = StringUtils.upperCase(columnType);
    int currentIndex = 0;
    int arrayDepth = 0;
    boolean isNullable = false;
    FireboltDataType arrayType = null;
    List<FireboltColumn> tupleDataTypes = null;
    Optional<Pair<Optional<Integer>, Optional<Integer>>> scaleAndPrecisionPair;
    FireboltDataType fireboltType;
    if (typeInUpperCase.startsWith(FireboltDataType.TUPLE.getInternalName().toUpperCase())) {
      tupleDataTypes = getTupleBaseDateTypes(typeInUpperCase, columnName);
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
        tupleDataTypes = getTupleBaseDateTypes(tmp.toUpperCase(), columnName);
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
        .tupleBaseDateTypes(tupleDataTypes)
        .build();
  }

  public static FireboltColumn of(String columnType) {
    return of(columnType, null);
  }

  private static List<FireboltColumn> getTupleBaseDateTypes(String columnType, String columnName) {
    return Arrays.stream(getTupleTypes(columnType))
        .map(String::trim)
        .map(type -> FireboltColumn.of(type, columnName))
        .collect(Collectors.toList());
  }

  @NonNull
  private static String[] getTupleTypes(String columnType) {
    String types =
        RegExUtils.replaceFirst(
            columnType, FireboltDataType.TUPLE.getInternalName().toUpperCase() + "\\(", "");
    types = StringUtils.substring(types, 0, types.length() - 1);
    return types.split(
        ",(?![^()]*\\))"); // Regex to split on comma and ignoring comma that are between
                           // parenthesis
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
      return getTupleCompactTypeName(this.tupleBaseDateTypes);
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
      type.append(this.getTupleCompactTypeName(this.getTupleBaseDateTypes()));
    }
    for (int i = 0; i < arrayDepth; i++) {
      type.append(")");
    }
    return type.toString();
  }

  private String getTupleCompactTypeName(List<FireboltColumn> columnsTuple) {

    return columnsTuple.stream()
        .map(FireboltColumn::getCompactTypeName)
        .collect(Collectors.joining(", ", TUPLE.getDisplayName() + "(", ")"));
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
