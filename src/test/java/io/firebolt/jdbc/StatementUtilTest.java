package io.firebolt.jdbc;

import io.firebolt.jdbc.statement.StatementUtil;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class StatementUtilTest {

  @Test
  void shouldExtractAdditionalProperties() {
    String query = "set my_custom_query=1";
    assertEquals(
        Optional.of(new ImmutablePair<>("my_custom_query", "1")),
        StatementUtil.extractPropertyFromQuery(query));
  }

  @Test
  void shouldExtractAdditionalPropertiesWithComments() {
    String query = "/* */" + " SeT my_custom_query=1";
    assertEquals(
        Optional.of(new ImmutablePair<>("my_custom_query", "1")),
        StatementUtil.extractPropertyFromQuery(query));
  }

  @Test
  void shouldExtractAdditionalWithEmptyProperties() {
    String query = "set my_custom_char=' '";
    assertEquals(
        Optional.of(new ImmutablePair<>("my_custom_char", "' '")),
        StatementUtil.extractPropertyFromQuery(query));
  }

  @Test
  void shouldFindThatShowQueryIsASelect() {
    String query = "/* Some random command*/ SHOW DATABASES";
    assertTrue(StatementUtil.isQuery(query));
  }

  @Test
  void shouldFindThatWithQueryIsASelect() {
    String sql = getSqlFromFile("/queries/query-with-keyword-with.sql");
    assertTrue(StatementUtil.isQuery(sql));
  }

  @Test
  void shouldExtractTableNameFromQuery() {
    String query =
        "/* Some random comment*/ SELECT /* Second comment */ * FROM -- third comment \n EMPLOYEES WHERE id = 5";
    assertEquals(
        Optional.of("EMPLOYEES"),
        StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getRight());
  }

  @Test
  void shouldExtractDbNameFromQuery() {
    String query =
        "-- Some random command   \n       SELECT *     FROM    db.schema.EMPLOYEES      WHERE id = 5";
    assertEquals(
        Optional.of("db"), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getLeft());
  }

  @Test
  void shouldBeEmptyWhenGettingDbNameAndThereIsNoDbName() {
    String query = "/* Some random command*/ SELECT * FROM EMPLOYEES WHERE id = 5";
    assertEquals(
        Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getLeft());
  }

  @Test
  void shouldBeEmptyWhenGettingDbNameFromAQueryWithoutFrom() {
    String query = "SELECT *";
    assertEquals(
        Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getLeft());
  }

  @Test
  void shouldGetEmptyDbNameAndTablesTableNameWhenUsingDescribe() {
    String query = "DESCRIBE EMPLOYEES";
    assertEquals(
        Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getLeft());
    assertEquals(
        Optional.of("tables"),
        StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getRight());
  }

  @Test
  void shouldGetEmptyTableNameAndEmptyDbNameWhenUsingShow() {
    String query = "SHOW databases";
    assertEquals(
        Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getLeft());
    assertEquals(
        Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getRight());
  }

  @Test
  void shouldBeEmptyWhenGettingTableNameWhenTheQueryIsNotASelect() {
    String query = "/* Some random command*/ UPDATE * FROM EMPLOYEES WHERE id = 5";
    assertEquals(
        Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getRight());
  }

  @Test
  void shouldThrowAnExceptionWhenTheSetCannotBeParsed() {
    String query = "set x=";
    assertThrows(
        IllegalArgumentException.class, () -> StatementUtil.extractPropertyFromQuery(query));
  }

  @Test
  void shouldCleanQueryWithComments() {
    String sql = getSqlFromFile("/queries/query-with-comment.sql");
    String expectedCleanQuery = getSqlFromFile("/queries/query-with-comment-cleaned.sql");
    Pair<String, Integer> cleanQueryWithCount =
        StatementUtil.cleanQueryAndCountKeyWordOccurrences(sql, "?");
    assertEquals(expectedCleanQuery, cleanQueryWithCount.getLeft());
    assertEquals(1, cleanQueryWithCount.getRight());
  }

  @Test
  void shouldCleanQueryWithQuotesInTheVarchar() {
    String sql =
        "INSERT INTO regex_test (name)\n"
            + "-- Hello\n"
            + "VALUES (/* some comment */\n"
            + "'Taylor''s Prime Steak House 3' /* some comment */)--";
    String expectedCleanQuery =
        "INSERT INTO regex_test (name)\n" + "\n" + "VALUES (\n" + "'Taylor''s Prime Steak House 3'";
    String cleanQuery = StatementUtil.cleanQuery(sql);
    assertEquals(expectedCleanQuery, cleanQuery);
  }

  @Test
  void shouldCleanQueryWithSingleLineComment() {
    String sql = getSqlFromFile("/queries/query-with-comment.sql");
    String expectedCleanQuery = getSqlFromFile("/queries/query-with-comment-cleaned.sql");
    Pair<String, Integer> cleanQueryWithCount =
        StatementUtil.cleanQueryAndCountKeyWordOccurrences(sql, "?");
    assertEquals(expectedCleanQuery, cleanQueryWithCount.getLeft());
    assertEquals(1, cleanQueryWithCount.getRight());
  }

  private static String getSqlFromFile(String path) {
    InputStream is = StatementUtilTest.class.getResourceAsStream(path);
    return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
        .lines()
        .collect(Collectors.joining("\n"));
  }
}