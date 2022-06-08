package io.firebolt;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Optional;

@UtilityClass
@Slf4j
public class QueryUtil {

  private static final String SINGLE_LINE_COMMENTS_REGEX = "--[^\n]*\n";
  private static final String MULTI_LINE_COMMENTS_REGEX = "/\\*[^/\\*]*\\*/";
  private static final String ALL_COMMENTS_REGEX =
      SINGLE_LINE_COMMENTS_REGEX + "|" + MULTI_LINE_COMMENTS_REGEX;

  private static final String SET_PREFIX = "set";
  private static final String[] SELECT_KEYWORDS =
      new String[] {"show", "select", "describe", "exists", "explain"};

  public static boolean isSelect(String sql) {
    String cleanQuery = removeCommentsAndTrimQuery(sql);
    return StringUtils.startsWithAny(cleanQuery.toLowerCase(), SELECT_KEYWORDS);
  }

  public Optional<Pair<String, String>> extractAdditionalProperties(String sql) {
    String cleanQuery = removeCommentsAndTrimQuery(sql);
    if (cleanQuery.toLowerCase().startsWith(SET_PREFIX)) {
      return extractPropertyPair(sql, cleanQuery);
    } else {
      return Optional.empty();
    }
  }

  public String removeCommentsAndTrimQuery(String sql) {
    return RegExUtils.replaceAll(sql, ALL_COMMENTS_REGEX, "")
        .trim(); // Replace substrings that starts with -- and ends with \n OR /* and ends with */
  }

  private Optional<Pair<String, String>> extractPropertyPair(String sql, String query) {
    String setQuery = StringUtils.stripStart(query, SET_PREFIX + " ");
    String[] values = StringUtils.split(setQuery, "=");
    if (values.length == 2) {
      return Optional.of(Pair.of(values[0].trim(), values[1].trim()));
    } else {
      throw new IllegalArgumentException(
          "Cannot parse the additional properties provided in the query: " + sql);
    }
  }

  public static Optional<String> extractDBNameFromSelect(String sql) {
    return extractDBAndTableNameFromSelect(sql)
        .filter(dbNameAndTable -> dbNameAndTable.contains("."))
        .map(dbNameAndTable -> dbNameAndTable.substring(0, dbNameAndTable.indexOf(".")));
  }

  public static Optional<String> extractTableNameFromSelect(String sql) {
    return extractDBAndTableNameFromSelect(sql)
        .map(
            dbNameAndTable -> {
              if (dbNameAndTable.contains(".")) {
                return dbNameAndTable.substring(dbNameAndTable.indexOf(".") + 1);
              } else {
                return dbNameAndTable;
              }
            });
  }

  private static Optional<String> extractDBAndTableNameFromSelect(String sql) {
    if (isSelect(sql)) {
      log.debug("Extracting DB and Table name for SELECT: {}", sql);
      String cleanQuery = removeCommentsAndTrimQuery(sql);
      String withoutQuotes = StringUtils.replace(cleanQuery, "'", "").trim();
      if (StringUtils.startsWithIgnoreCase(withoutQuotes, "select")) {
        int fromIndex = StringUtils.indexOfIgnoreCase(withoutQuotes, "from");
        if (fromIndex != -1) {
          return Optional.of(
              withoutQuotes.substring(fromIndex + "from".length()).trim().split(" ")[0]);
        }
      }
      if (StringUtils.startsWithIgnoreCase(withoutQuotes, "DESCRIBE")) {
        return Optional.of("information_schema.tables");
      }
      if (StringUtils.startsWithIgnoreCase(withoutQuotes, "SHOW")) {
        return Optional.of("information_schema.unknown"); // Depends on the information requested - so putting unknown as a placeholder
      }
    }
    log.debug("Could not find table name for query {}. This may happen when there is no table.", sql);
    return Optional.empty();
  }
}
