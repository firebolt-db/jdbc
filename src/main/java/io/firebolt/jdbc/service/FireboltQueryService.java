package io.firebolt.jdbc.service;

import io.firebolt.jdbc.client.query.QueryClient;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.InputStream;
import java.util.Optional;

@RequiredArgsConstructor
@Slf4j
public class FireboltQueryService {
  private static final String SET_PREFIX = "set ";
  private final QueryClient queryClient;

  public InputStream executeQuery (
      String sql, String queryId, String accessToken, FireboltProperties properties) throws FireboltException {
    log.debug("Executing SQL: {}", sql);
    return queryClient.postSqlQuery(sql, queryId, accessToken, properties);
  }

  public Optional<Pair<String, String>> extractAdditionalProperties(String sql) {
    sql = sql.toLowerCase().trim();
    int i = 0;
    while (i < sql.length()) {
      String nextTwoChars = sql.substring(i, Math.min(i + 2, sql.length()));
      switch (nextTwoChars) {
        case "--":
          i = Math.max(i, sql.indexOf("\n", i));
          break;
        case "/*":
          i = Math.max(i, sql.indexOf("*/", i));
          break;
        default:
          String trimmedQuery = sql.substring(i).trim().toLowerCase();
          if (StringUtils.startsWithIgnoreCase(trimmedQuery, SET_PREFIX)) {
            return extractPropertyPair(sql, trimmedQuery);
          }
          return Optional.empty();
      }
      i++;
    }
    return Optional.empty();
  }

  private Optional<Pair<String, String>> extractPropertyPair(String sql, String query) {
    String setQuery = StringUtils.stripStart(query, SET_PREFIX);
    String[] values = StringUtils.split(setQuery, "=");
    if (values.length == 2) {
      return Optional.of(Pair.of(values[0], values[1]));
    } else {
      throw new IllegalArgumentException(
          "Cannot parse the additional properties provided in the query: " + sql);
    }
  }

  public Optional<String> extractDBName(String sql) {
    String s = extractDBAndTableName(sql);
    if (s.contains(".")) {
      return Optional.of(s.substring(0, s.indexOf(".")));
    } else {
      return Optional.empty();
    }
  }

  public String extractTableName(String sql) {
    String s = extractDBAndTableName(sql);
    if (s.contains(".")) {
      return s.substring(s.indexOf(".") + 1);
    } else {
      return s;
    }
  }

  private String extractDBAndTableName(String sql) {
    String withoutQuotes = StringUtils.replace(sql, "'", "");
    if (StringUtils.startsWithIgnoreCase(sql, "select")) {
      int fromIndex = StringUtils.indexOfIgnoreCase(sql, "from");
      if (fromIndex != -1) {
        String fromFrom = withoutQuotes.substring(fromIndex);
        String fromTable = fromFrom.substring("from".length()).trim();
        return fromTable.split(" ")[0];
      }
    }
    if (StringUtils.startsWith(sql, "desc")) {
      return "system.columns";
    }
    if (StringUtils.startsWith(sql, "show")) {
      return "system.tables";
    }
    return "system.unknown";
  }
}
