package io.firebolt.jdbc;

import io.firebolt.QueryUtil;
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

class QueryUtilTest {

  @Test
  void shouldExtractAdditionalProperties() {
    String query = "set my_custom_query=1";
    assertEquals(
        Optional.of(new ImmutablePair<>("my_custom_query", "1")),
        QueryUtil.extractAdditionalProperties(query));
  }

  @Test
  void shouldExtractAdditionalPropertiesWithComments() {
    String query = "/* */" + " SeT my_custom_query=1";
    assertEquals(
        Optional.of(new ImmutablePair<>("my_custom_query", "1")),
        QueryUtil.extractAdditionalProperties(query));
  }

  @Test
  void shouldExtractAdditionalWithEmptyProperties() {
    String query = "set my_custom_char=' '";
    assertEquals(
        Optional.of(new ImmutablePair<>("my_custom_char", "' '")),
        QueryUtil.extractAdditionalProperties(query));
  }

  @Test
  void shouldFindThatShowQueryIsASelect() {
    String query = "/* Some random command*/ SHOW DATABASES";
    assertTrue(QueryUtil.isSelect(query));
  }

  @Test
  void shouldFindThatWithQueryIsASelect() {
    String sql = getSqlFromFile("/queries/query-with-keyword-with.sql");
    assertTrue(QueryUtil.isSelect(sql));
  }

  @Test
  void shouldExtractTableNameFromQuery() {
    String query = "/* Some random comment*/ SELECT /* Second comment */ * FROM -- third comment \n EMPLOYEES WHERE id = 5";
    assertEquals(Optional.of("EMPLOYEES"), QueryUtil.extractTableNameFromSelect(query));
  }

  @Test
  void shouldExtractDbNameFromQuery() {
    String query =
        "-- Some random command   \n       SELECT *     FROM    db.EMPLOYEES      WHERE id = 5";
    assertEquals(Optional.of("db"), QueryUtil.extractDBNameFromSelect(query));
  }

  @Test
  void shouldBeEmptyWhenGettingDbNameAndThereIsNoDbName() {
    String query = "/* Some random command*/ SELECT * FROM EMPLOYEES WHERE id = 5";
    assertEquals(Optional.empty(), QueryUtil.extractDBNameFromSelect(query));
  }

  @Test
  void shouldBeEmptyWhenGettingDbNameFromAQueryWithoutFrom() {
    String query = "SELECT *";
    assertEquals(Optional.empty(), QueryUtil.extractDBNameFromSelect(query));
  }

  @Test
  void shouldGetInformationSchemaAndTableWhenUsingDescribe() {
    String query = "DESCRIBE EMPLOYEES";
    assertEquals(Optional.of("information_schema"), QueryUtil.extractDBNameFromSelect(query));
    assertEquals(Optional.of("tables"), QueryUtil.extractTableNameFromSelect(query));
  }

  @Test
  void shouldGetInformationSchemaAndTableWhenUsingShow() {
    String query = "SHOW databases";
    assertEquals(Optional.of("information_schema"), QueryUtil.extractDBNameFromSelect(query));
    assertEquals(Optional.of("unknown"), QueryUtil.extractTableNameFromSelect(query));
  }

  @Test
  void shouldBeEmptyWhenGettingTableNameWhenTheQueryIsNotASelect() {
    String query = "/* Some random command*/ UPDATE * FROM EMPLOYEES WHERE id = 5";
    assertEquals(Optional.empty(), QueryUtil.extractTableNameFromSelect(query));
  }

  @Test
  void shouldThrowAnExceptionWhenTheSetCannotBeParsed() {
    String query = "set x=";
    assertThrows(IllegalArgumentException.class, () -> QueryUtil.extractAdditionalProperties(query));
  }

  @Test
  void shouldCleanQueryWithComments() {
    String sql = getSqlFromFile("/queries/query-with-comment.sql");
    String expectedCleanQuery = getSqlFromFile("/queries/query-with-comment-cleaned.sql");
    Pair<String, Integer> cleanQueryWithCount =  QueryUtil.cleanQueryAndCountKeyWordOccurrences(sql, "?");
    assertEquals(expectedCleanQuery, cleanQueryWithCount.getLeft());
    assertEquals(1, cleanQueryWithCount.getRight());
  }

  private static String getSqlFromFile(String path) {
    InputStream is = QueryUtilTest.class.getResourceAsStream(path);
    return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));
  }
}
