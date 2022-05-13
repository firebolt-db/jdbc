package io.firebolt.jdbc.resultset;

import io.firebolt.jdbc.resultset.type.FireboltDataType;
import lombok.Builder;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Optional;

import static io.firebolt.jdbc.resultset.type.FireboltDataType.NULLABLE_TYPE;

@Builder
@Getter
public final class FireboltColumn {

  private final String columnType;
  private final String columnName;
  private FireboltDataType fireboltDataType;
  private boolean nullable;

  private FireboltDataType arrayType;
  private int arrayDepth;

  private int precision;
  private int scale;

  public static FireboltColumn of(String columnType, String columnName) {
    int currentIndex = 0;
    int arrayDepth = 0;
    boolean isNullable = false;
    FireboltDataType arrayType = null;
    Optional<Pair<Optional<Integer>, Optional<Integer>>> scaleAndPrecisionPair;
    FireboltDataType fireboltType;

    while (columnType.startsWith(FireboltDataType.ARRAY.getName(), currentIndex)) {
      arrayDepth++;
      currentIndex += FireboltDataType.ARRAY.getName().length() + 1;
    }

    if (columnType.startsWith(NULLABLE_TYPE, currentIndex)) {
      isNullable = true;
      currentIndex += NULLABLE_TYPE.length() + 1;
    }
    int typeEndIndex = getTypeEndPosition(columnType, currentIndex);
    FireboltDataType dataType =
        FireboltDataType.ofType(columnType.substring(currentIndex, typeEndIndex));
    if (arrayDepth > 0) {
      arrayType = dataType;
      fireboltType = FireboltDataType.ARRAY;
    } else {
      fireboltType = dataType;
    }

    if (!reachedEndOfTypeName(typeEndIndex, columnType.length())
        || columnType.startsWith("(", typeEndIndex)) {
      String[] arguments = splitArguments(columnType, typeEndIndex);
      scaleAndPrecisionPair = Optional.of(getsCaleAndPrecision(arguments, dataType));
    } else {
      scaleAndPrecisionPair = Optional.empty();
    }

    return FireboltColumn.builder()
        .columnName(columnName)
        .columnType(columnType)
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
        .arrayType(arrayType)
        .fireboltDataType(fireboltType)
        .nullable(isNullable)
        .arrayDepth(arrayDepth)
        .build();
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
          precision = Integer.parseInt(arguments[0]);
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
    if (!nullable) {
      return columnType;
    } else {
      String trimmedType = StringUtils.remove(columnType, NULLABLE_TYPE + "(");
      return StringUtils.removeEnd(trimmedType, ")");
    }
  }
}
