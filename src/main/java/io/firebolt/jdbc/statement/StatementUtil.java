package io.firebolt.jdbc.statement;

import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

@UtilityClass
@Slf4j
public class StatementUtil {

  private static final String SINGLE_LINE_COMMENTS_REGEX = "--[^\n]*\n";
  private static final String MULTI_LINE_COMMENTS_REGEX = "/\\*[^/\\*]*\\*/";

  private static final String SET_PREFIX = "set";
  private static final Pattern SET_WITH_SPACE_REGEX =
      Pattern.compile(SET_PREFIX + " ", Pattern.CASE_INSENSITIVE);
  private static final String[] SELECT_KEYWORDS =
      new String[] {"show", "select", "describe", "exists", "explain", "with"};

  public static StatementInfoWrapper extractStatementInfo(String sql, String statementId) {
    String cleaned = StatementUtil.cleanStatement(sql);
    Optional<Pair<String, String>> additionalProperties =
        extractPropertyFromQuery(cleaned, statementId);
    StatementInfoWrapper.StatementType statementType;
    if (additionalProperties.isPresent()) {
      statementType = StatementInfoWrapper.StatementType.PARAM_SETTING;
    } else {
      statementType =
          StatementUtil.isQuery(cleaned, true)
              ? StatementInfoWrapper.StatementType.QUERY
              : StatementInfoWrapper.StatementType.NON_QUERY;
    }
    return StatementInfoWrapper.builder()
        .sql(sql)
        .id(statementId)
        .sql(sql)
        .cleanSql(cleaned)
        .type(statementType)
        .param(additionalProperties.orElse(null))
        .build();
  }

  public static boolean isQuery(String sql) {
    return isQuery(sql, false);
  }

  public static boolean isQuery(String sql, boolean isCleanStatement) {
    if (StringUtils.isNotEmpty(sql)) {
      String cleanQuery = isCleanStatement ? sql : cleanStatement(sql);
      cleanQuery = cleanQuery.replace("(", "");
      return StringUtils.startsWithAny(cleanQuery.toLowerCase(), SELECT_KEYWORDS);
    } else {
      return false;
    }
  }

  public Optional<Pair<String, String>> extractPropertyFromQuery(
      @NonNull String cleanStatement, String sql) {
    if (StringUtils.startsWithIgnoreCase(cleanStatement, SET_PREFIX)) {
      return extractPropertyPair(cleanStatement, sql);
    }
    return Optional.empty();
  }

  public String cleanStatement(String sql) {
    return cleanQueryAndCountKeyWordOccurrences(sql, null).getLeft();
  }

  public Pair<String, Integer> cleanQueryAndCountKeyWordOccurrences(String sql, String keyword) {
    int searchedWordCount = 0;
    StringBuilder result = new StringBuilder();
    sql = sql.trim();
    int currentIndex = 0;
    char currentChar = sql.charAt(currentIndex);
    boolean isCurrentSubstringBetweenQuotes = currentChar == '\'';
    int substringStart = 0;
    boolean isInSingleLineComment = false;
    boolean isInMultipleLinesComment = false;
    boolean isInComment;
    char previousChar;
    Integer latestCommentPos = null;
    while (currentIndex < sql.length() - 1) {
      currentIndex++;
      previousChar = currentChar;
      currentChar = sql.charAt(currentIndex);
      isInSingleLineComment =
          isInSingleLineComment(
              currentChar, previousChar, isCurrentSubstringBetweenQuotes, isInSingleLineComment);
      isInMultipleLinesComment =
          isInMultipleLinesComment(
              currentChar, previousChar, isCurrentSubstringBetweenQuotes, isInMultipleLinesComment);
      isInComment = isInSingleLineComment || isInMultipleLinesComment;
      if (latestCommentPos == null && isInComment) {
        latestCommentPos = currentIndex - 1;
      }

      if (('\'' == currentChar && !isInComment) || reachedEnd(sql, currentIndex)) {
        if (isCurrentSubstringBetweenQuotes) {
          String subString = StringUtils.substring(sql, substringStart, currentIndex + 1);
          result.append(subString);
        } else {
          int subStringEnd = isInComment ? latestCommentPos : currentIndex + 1;
          Pair<String, Integer> cleanSubstring =
              cleanQueryPartAndCountWordOccurrences(sql, substringStart, subStringEnd, keyword);
          result.append(cleanSubstring.getLeft());
          searchedWordCount += cleanSubstring.getRight();
        }
        substringStart = currentIndex + 1;
        isCurrentSubstringBetweenQuotes = !isCurrentSubstringBetweenQuotes;
        latestCommentPos = null;
      }
    }
    return new ImmutablePair<>(result.toString().trim(), searchedWordCount);
  }

  public Map<Integer, Integer> getQueryParamsPositions(String sql) {
    Map<Integer, Integer> queryParams = new HashMap<>();
    int currentIndex = 0;
    char currentChar = sql.charAt(currentIndex);
    boolean isCurrentSubstringBetweenQuotes = currentChar == '\'';
    boolean isInSingleLineComment = false;
    boolean isInMultipleLinesComment = false;
    boolean isInComment;
    char previousChar;
    int count = 0;
    while (currentIndex++ < sql.length() - 1) {
      previousChar = currentChar;
      currentChar = sql.charAt(currentIndex);
      isInSingleLineComment =
          isInSingleLineComment(
              currentChar, previousChar, isCurrentSubstringBetweenQuotes, isInSingleLineComment);
      isInMultipleLinesComment =
          isInMultipleLinesComment(
              currentChar, previousChar, isCurrentSubstringBetweenQuotes, isInMultipleLinesComment);
      isInComment = isInSingleLineComment || isInMultipleLinesComment;
      if (!isInComment) {
        if (currentChar == '?' && !isCurrentSubstringBetweenQuotes) {
          queryParams.put(++count, currentIndex);
        } else if (currentChar == '\'') {
          isCurrentSubstringBetweenQuotes = !isCurrentSubstringBetweenQuotes;
        }
      }
    }
    return queryParams;
  }

  private boolean reachedEnd(String sql, int currentIndex) {
    return currentIndex == sql.length() - 1;
  }

  private static boolean isInMultipleLinesComment(
      char currentChar,
      char previousChar,
      boolean isCurrentSubstringBetweenQuotes,
      boolean isInMultipleLinesComment) {
    if (!isCurrentSubstringBetweenQuotes && (previousChar == '/' && currentChar == '*')) {
      return true;
    } else if ((previousChar == '*' && currentChar == '/')) {
      return false;
    }
    return isInMultipleLinesComment;
  }

  public static Pair<Optional<String>, Optional<String>> extractDbNameAndTableNamePairFromQuery(
      String sql) {
    Optional<String> from = Optional.empty();
    if (isQuery(sql)) {
      log.debug("Extracting DB and Table name for SELECT: {}", sql);
      String cleanQuery = cleanStatement(sql);
      String withoutQuotes = StringUtils.replace(cleanQuery, "'", "").trim();
      if (StringUtils.startsWithIgnoreCase(withoutQuotes, "select")) {
        int fromIndex = StringUtils.indexOfIgnoreCase(withoutQuotes, "from");
        if (fromIndex != -1) {
          from =
              Optional.of(
                  withoutQuotes.substring(fromIndex + "from".length()).trim().split(" ")[0]);
        }
      } else if (StringUtils.startsWithIgnoreCase(withoutQuotes, "DESCRIBE")) {
        from = Optional.of("tables");
      } else if (StringUtils.startsWithIgnoreCase(withoutQuotes, "SHOW")) {
        from = Optional.empty(); // Depends on the information requested
      } else {
        log.debug(
            "Could not find table name for query {}. This may happen when there is no table.", sql);
      }
    }
    return new ImmutablePair<>(
        extractDbNameFromFromPartOfTheQuery(from.orElse(null)),
        extractTableNameFromFromPartOfTheQuery(from.orElse(null)));
  }

  private static Optional<String> extractTableNameFromFromPartOfTheQuery(String from) {
    return Optional.ofNullable(from)
        .map(s -> s.replace("\"", ""))
        .map(
            fromPartOfTheQuery -> {
              if (StringUtils.contains(fromPartOfTheQuery, ".")) {
                int indexOfTableName = StringUtils.lastIndexOf(fromPartOfTheQuery, ".");
                return fromPartOfTheQuery.substring(indexOfTableName + 1);
              } else {
                return fromPartOfTheQuery;
              }
            });
  }

  private static Optional<String> extractDbNameFromFromPartOfTheQuery(String from) {
    return Optional.ofNullable(from)
        .map(s -> s.replace("\"", ""))
        .filter(s -> StringUtils.countMatches(s, ".") == 2)
        .map(
            fromPartOfTheQuery -> {
              int dbNameEndPos = StringUtils.indexOf(fromPartOfTheQuery, ".");
              return fromPartOfTheQuery.substring(0, dbNameEndPos);
            });
  }

  private boolean isInSingleLineComment(
      char currentChar,
      char previousChar,
      boolean isCurrentSubstringBetweenQuotes,
      boolean isInSingleLineComment) {
    if (!isCurrentSubstringBetweenQuotes && (previousChar == '-' && currentChar == '-')) {
      return true;
    } else if (currentChar == '\n') {
      return false;
    }
    return isInSingleLineComment;
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
    subString = RegExUtils.replaceAll(subString, MULTI_LINE_COMMENTS_REGEX, "");
    return RegExUtils.replaceAll(
        subString,
        SINGLE_LINE_COMMENTS_REGEX,
        "\n"); // Escape to next line to avoid words being merged when comments are added at the end
    // of the line
  }

  private Optional<Pair<String, String>> extractPropertyPair(String cleanStatement, String sql) {
    String setQuery = RegExUtils.removeFirst(cleanStatement, SET_WITH_SPACE_REGEX);
    String[] values = StringUtils.split(setQuery, "=");
    if (values.length == 2) {
      return Optional.of(Pair.of(values[0].trim(), values[1].trim()));
    } else {
      throw new IllegalArgumentException(
          "Cannot parse the additional properties provided in the statement: " + sql);
    }
  }
}
