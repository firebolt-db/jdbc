package io.firebolt;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
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
    String cleanQuery = cleanQuery(sql);
    return StringUtils.startsWithAny(cleanQuery.toLowerCase(), SELECT_KEYWORDS);
  }

  public Optional<Pair<String, String>> extractAdditionalProperties(String sql) {
    String cleanQuery = cleanQuery(sql);
    if (cleanQuery.toLowerCase().startsWith(SET_PREFIX)) {
      return extractPropertyPair(sql, cleanQuery);
    } else {
      return Optional.empty();
    }
  }

  public String cleanQuery(String sql) {
    return cleanQueryAndCountUnquotedWordOccurrences(sql, null).getLeft();
  }

  public Pair<String, Integer> cleanQueryAndCountUnquotedWordOccurrences(String sql, String searchedWord) {
    int searchedWordCount = 0;
    StringBuilder result = new StringBuilder();
    sql = sql.trim();
    int currentIndex = 0;
    char currentChar = sql.charAt(currentIndex);
    boolean isCurrentSubstringBetweenQuotes = currentChar == 39; // 39 is the ASCII for '
    int substringStart = 0;
    while (currentIndex < sql.length() - 1) {
      currentIndex++;
      currentChar = sql.charAt(currentIndex);
      if (39 == currentChar) {
        if (isCurrentSubstringBetweenQuotes) {
          String subString = StringUtils.substring(sql, substringStart, currentIndex + 1);
          result.append(subString);
        } else {
          Pair<String, Integer> cleanSubstring =
              cleanQueryPartAndCountWordOccurrences(
                  sql, substringStart, currentIndex + 1, searchedWord);
          result.append(cleanSubstring.getLeft());
          searchedWordCount += cleanSubstring.getRight();
        }
        substringStart = currentIndex + 1;
        isCurrentSubstringBetweenQuotes = !isCurrentSubstringBetweenQuotes;
      }
    }
    if (substringStart < sql.length()) {
      Pair<String, Integer> cleanSubstring =
          cleanQueryPartAndCountWordOccurrences(
              sql, substringStart, sql.length() + 1, searchedWord);
      result.append(cleanSubstring.getLeft());
      searchedWordCount += cleanSubstring.getRight();
    }
    return new ImmutablePair<>(result.toString().trim(), searchedWordCount);
  }

  private Pair<String, Integer> cleanQueryPartAndCountWordOccurrences(
      String sql, int substringStart, int substringEnd, String searchedWord) {
    String subString = StringUtils.substring(sql, substringStart, substringEnd);
    int count = 0;
    String unquotedWord = removeCommentsFromSubstring(subString);
    if (searchedWord != null) {
      count = StringUtils.countMatches(unquotedWord, searchedWord);
    }
    return new ImmutablePair<>(unquotedWord, count);
  }

  private String removeCommentsFromSubstring(String subString) {
    return RegExUtils.replaceAll(subString, ALL_COMMENTS_REGEX, "");
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
      String cleanQuery = cleanQuery(sql);
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
        return Optional.of(
            "information_schema.unknown"); // Depends on the information requested - so putting
        // unknown as a placeholder
      }
    }
    log.debug(
        "Could not find table name for query {}. This may happen when there is no table.", sql);
    return Optional.empty();
  }
}
