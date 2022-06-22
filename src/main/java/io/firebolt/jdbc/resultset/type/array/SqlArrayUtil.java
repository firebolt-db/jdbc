package io.firebolt.jdbc.resultset.type.array;

import com.google.common.base.CharMatcher;
import io.firebolt.jdbc.resultset.type.FireboltDataType;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

@UtilityClass
@Slf4j
public class SqlArrayUtil {

  private static final String SEPARATOR_COMMA = ",";

  public static final BiFunction<String, FireboltDataType, java.sql.Array>
      transformToSqlArrayFunction =
          (value, fireboltComplexDataType) -> {
            int dimensions = 0;
            for (int x = 0; x < value.length(); x++)
              if (value.charAt(x) == '[') dimensions++;
              else break;
            value = value.substring(dimensions, value.length() - dimensions);
            Object arr = createArray(value, dimensions, fireboltComplexDataType);
            return FireboltArray.builder().array(arr).type(fireboltComplexDataType).build();
          };

  private static Object createArray(String str, int dimension, FireboltDataType arraySubType) {
    if (dimension == 1) {
      return extractArrayFromOneDimensionalArray(str, arraySubType);
    } else {
      return extractArrayFromMultiDimensionalArray(str, dimension, arraySubType);
    }
  }

  @NotNull
  private static Object extractArrayFromMultiDimensionalArray(
      String str, int dimension, FireboltDataType arraySubType) {
    String[] s = str.split(getArraySeparator(dimension));
    int[] lengths = new int[dimension];
    lengths[0] = s.length;
    Object currentArray = Array.newInstance(arraySubType.getBaseType().getType(), lengths);

    for (int x = 0; x < s.length; x++)
      Array.set(currentArray, x, createArray(s[x], dimension - 1, arraySubType));

    return currentArray;
  }

  private static Object extractArrayFromOneDimensionalArray(
      String str, FireboltDataType arraySubType) {
    List<String> values =
        Arrays.stream(str.split(SEPARATOR_COMMA))
            .map(SqlArrayUtil::removeQuotes)
            .collect(Collectors.toList());
    Object currentArray = Array.newInstance(arraySubType.getBaseType().getType(), values.size());
    for (int x = 0; x < values.size(); x++)
      Array.set(currentArray, x, arraySubType.getBaseType().transform(values.get(x), null));
    return currentArray;
  }

  private static String getArraySeparator(int dimension) {
    StringBuilder stringBuilder = new StringBuilder(SEPARATOR_COMMA);
    for (int x = 1; x < dimension; x++) {
      stringBuilder.insert(0, ']');
      stringBuilder.append("\\[");
    }
    return stringBuilder.toString();
  }

  private static String removeQuotes(String s) {
    return CharMatcher.is('\'').trimFrom(s);
  }

  public static String arrayToString(Object object)
  {
    throw new UnsupportedOperationException();
  }

}
