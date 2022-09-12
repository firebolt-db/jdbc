package com.firebolt.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.statement.StatementUtil;
import com.firebolt.jdbc.statement.rawstatement.SqlParamMarker;
import com.google.common.collect.ImmutableMap;

class StatementUtilTest {

	private static String getSqlFromFile(String path) {
		InputStream is = StatementUtilTest.class.getResourceAsStream(path);
		return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)).lines()
				.collect(Collectors.joining("\n"));
	}

	@Test
	void shouldExtractAdditionalProperties() {
		String query = "set my_custom_query=1";
		assertEquals(Optional.of(new ImmutablePair<>("my_custom_query", "1")),
				StatementUtil.extractPropertyFromQuery(query, null));
	}

	@Test
	void shouldExtractAdditionalPropertiesWithComments() {
		String query = "/* */" + " SeT my_custom_query=1";
		String cleanQuery = "SeT my_custom_query=1";
		assertEquals(Optional.of(new ImmutablePair<>("my_custom_query", "1")),
				StatementUtil.extractPropertyFromQuery(cleanQuery, query));
	}

	@Test
	void shouldExtractAdditionalWithEmptyProperties() {
		String query = "set my_custom_char=' '";
		assertEquals(Optional.of(new ImmutablePair<>("my_custom_char", "' '")),
				StatementUtil.extractPropertyFromQuery(query, null));
	}

	@Test
	void shouldFindThatStatementsWithQueryKeywordsAreQueries() {
		List<String> keywords = Arrays.asList("shOW", "seleCt", "DESCRIBE", "exists", "explain", "with", "call");

		String query = "/* Some random command*/ -- oneLineOfComment INSERT \n %s anything";
		keywords.forEach(keyword -> assertTrue(StatementUtil.isQuery(String.format(query, keyword))));
	}

	@Test
	void shouldFindThatStatementWithoutQueryKeywordsAreNotQueries() {
		List<String> keywords = Arrays.asList("Insert", "updAte", "Delete", "hello");
		String query = "/* Some random command SELECT */ -- oneLineOfComment SELECT \n %s anything";
		keywords.forEach(keyword -> assertFalse(StatementUtil.isQuery(String.format(query, keyword))));
	}

	@Test
	void shouldExtractTableNameFromQuery() {
		String query = "/* Some random comment*/ SELECT /* Second comment */ * FROM -- third comment \n EMPLOYEES WHERE id = 5";
		assertEquals(Optional.of("EMPLOYEES"), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getRight());
	}

	@Test
	void shouldExtractDbNameFromQuery() {
		String query = "-- Some random command   \n       SELECT *     FROM    db.schema.EMPLOYEES      WHERE id = 5";
		assertEquals(Optional.of("db"), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getLeft());
	}

	@Test
	void shouldBeEmptyWhenGettingDbNameAndThereIsNoDbName() {
		String query = "/* Some random command*/ SELECT * FROM EMPLOYEES WHERE id = 5";
		assertEquals(Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getLeft());
	}

	@Test
	void shouldBeEmptyWhenGettingDbNameFromAQueryWithoutFrom() {
		String query = "SELECT *";
		assertEquals(Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getLeft());
	}

	@Test
	void shouldGetEmptyDbNameAndTablesTableNameWhenUsingDescribe() {
		String query = "DESCRIBE EMPLOYEES";
		assertEquals(Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getLeft());
		assertEquals(Optional.of("tables"), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getRight());
	}

	@Test
	void shouldGetEmptyTableNameAndEmptyDbNameWhenUsingShow() {
		String query = "SHOW databases";
		assertEquals(Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getLeft());
		assertEquals(Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getRight());
	}

	@Test
	void shouldBeEmptyWhenGettingTableNameWhenTheQueryIsNotASelect() {
		String query = "/* Some random command*/ UPDATE * FROM EMPLOYEES WHERE id = 5";
		assertEquals(Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromQuery(query).getRight());
	}

	@Test
	void shouldThrowAnExceptionWhenTheSetCannotBeParsed() {
		String query = "set x=";
		assertThrows(IllegalArgumentException.class, () -> StatementUtil.extractPropertyFromQuery(query, null));
	}

	@Test
	void shouldCleanQueryWithComments() {
		String sql = getSqlFromFile("/queries/query-with-comment.sql");
		String expectedCleanQuery = getSqlFromFile("/queries/query-with-comment-cleaned.sql");
		String cleanStatement = StatementUtil.cleanStatement(sql);
		assertEquals(expectedCleanQuery, cleanStatement);
		assertEquals(expectedCleanQuery, StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().get(0).getCleanSql());
	}

	@Test
	void shouldCleanQueryWithQuotesInTheVarchar() {
		String sql = "INSERT INTO regex_test (name)\n" + "-- Hello\n" + "VALUES (/* some comment */\n"
				+ "'Taylor''s Prime Steak House 3' /* some comment */)--";
		String expectedCleanQuery = "INSERT INTO regex_test (name)\n" + "\n" + "VALUES (\n"
				+ "'Taylor''s Prime Steak House 3' )";
		assertEquals(expectedCleanQuery, StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().get(0).getCleanSql());

	}

	@Test
	void shouldCleanQueryWithSingleLineComment() {
		String sql = getSqlFromFile("/queries/query-with-comment.sql");
		String expectedCleanQuery = getSqlFromFile("/queries/query-with-comment-cleaned.sql");
		String cleanStatement = StatementUtil.cleanStatement(sql);
		assertEquals(expectedCleanQuery, cleanStatement);
		assertEquals(expectedCleanQuery, StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().get(0).getCleanSql());

	}

	@Test
	void shouldCountParametersFromLongQueryWithComments() {
		String sql = getSqlFromFile("/queries/query-with-comment.sql");
		StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().get(0).getSql();
		assertEquals(Arrays.asList(new SqlParamMarker(1, 200)),
				StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().get(0).getParamMarkers());
		assertEquals(1, StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParams() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ?";
		assertEquals(ImmutableMap.of(1, 35), StatementUtil.getQueryParamsPositions(sql));
		assertEquals(1, StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsWithoutTrimmingRequest() {
		String sql = "     SElECT * FROM EMPLOYEES WHERE id = ?";
		assertEquals(ImmutableMap.of(1, 40), StatementUtil.getQueryParamsPositions(sql));
		assertEquals(1, StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsFromIn() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN (?,?)";
		assertEquals(ImmutableMap.of(1, 37, 2, 39), StatementUtil.getQueryParamsPositions(sql));
		assertEquals(1, StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInComments() {
		String sql = "SElECT * FROM EMPLOYEES WHERE /* ?*/id IN (?,?)";
		assertEquals(ImmutableMap.of(1, 43, 2, 45), StatementUtil.getQueryParamsPositions(sql));
		assertEquals(1, StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInComments2() {
		String sql = "SElECT * FROM EMPLOYEES WHERE /* ?id IN (?,?)*/";
		assertEquals(ImmutableMap.of(), StatementUtil.getQueryParamsPositions(sql));
		assertEquals(1, StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInSingleLineComment() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n";
		assertEquals(ImmutableMap.of(), StatementUtil.getQueryParamsPositions(sql));
		assertEquals(1, StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInSingleLineComment2() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --\n(?,?)";
		assertEquals(ImmutableMap.of(1, 40, 2, 42), StatementUtil.getQueryParamsPositions(sql));
		assertEquals(1, StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInBetweenQuotesOrComments() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE ? AND my_date = ?";
		assertEquals(ImmutableMap.of(1, 93, 2, 109), StatementUtil.getQueryParamsPositions(sql));
		assertEquals(1, StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldReplaceOneQueryParamsThatAreNotInBetweenQuotesOrComments() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE ?";
		String expectedSql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE '55 Liverpool road%'";
		Map<Integer, String> params = ImmutableMap.of(1, "'55 Liverpool road%'");
		assertEquals(expectedSql,
				StatementUtil.replaceParameterMarksWithValues(params, sql).get(0).getSql());
	}

	@Test
	void shouldReplaceAQueryParam() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id is ?";
		String expectedSql = "SElECT * FROM EMPLOYEES WHERE id is 5";
		Map<Integer, String> params = ImmutableMap.of(1, "5");
		assertEquals(expectedSql,
				StatementUtil.replaceParameterMarksWithValues(params, sql).get(0).getSql());
	}

	@Test
	void shouldReplaceMultipleQueryParam() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? ";
		String expectedSql = "SElECT * FROM EMPLOYEES WHERE id = 5 AND name LIKE 'George' AND dob = '1980-05-22' ";
		Map<Integer, String> params = ImmutableMap.of(1, "5", 2, "'George'", 3, "'1980-05-22'");
		assertEquals(expectedSql,
				StatementUtil.replaceParameterMarksWithValues(params, sql).get(0).getSql());
	}

	@Test
	void shouldReplaceAllQueryParamsThatAreNotInBetweenQuotesOrComments() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE ? AND my_date = ? AND age = /* */ ?";
		String expectedSql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE '55 Liverpool road%' AND my_date = '2022-01-01' AND age = /* */ 5";
		Map<Integer, String> params = ImmutableMap.of(1, "'55 Liverpool road%'", 2, "'2022-01-01'", 3, "5");
		assertEquals(expectedSql,
				StatementUtil.replaceParameterMarksWithValues(params, sql).get(0).getSql());
	}

	@Test
	void shouldReplaceMultipleQueryParamsInaMultiStatement() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? ; SELECT * FROM cats WHERE name IN (?, ?)";
		String expectedFirstSql = "SElECT * FROM EMPLOYEES WHERE id = 5 AND name LIKE 'George' AND dob = '1980-05-22' ; ";
		String expectedSecondSql = "SELECT * FROM cats WHERE name IN ('Elizabeth', 'Charles')";

		Map<Integer, String> params = ImmutableMap.of(1, "5", 2, "'George'", 3, "'1980-05-22'", 4, "'Elizabeth'", 5,
				"'Charles'");
		assertEquals(expectedFirstSql,
				StatementUtil.replaceParameterMarksWithValues(params, sql).get(0).getSql());
		assertEquals(expectedSecondSql,
				StatementUtil.replaceParameterMarksWithValues(params, sql).get(1).getSql());

	}

	@Test
	void shouldThrowExceptionWhenTheNumberOfParamsIsNotTheSameAsTheNumberOfParamMarkers() {
		Map<Integer, String> params = ImmutableMap.of();
		assertThrows(IllegalArgumentException.class,
				() -> StatementUtil.replaceParameterMarksWithValues(params, "SELECT 1;"),
				"The number of parameters passed does not equal the number of parameter markers in the SQL query. Provided: 0, Parameter markers in the SQL query: 1");
	}

	@Test
	void shouldThrowExceptionWhenThePositionOfTheParamMarkerIsGreaterThanTheLengthOfTheStatement() {
		Map<Integer, String> params = ImmutableMap.of(1, "'test'");
		assertThrows(IllegalArgumentException.class, () -> StatementUtil
						.replaceParameterMarksWithValues(params, "SELECT 1;"),
				"The position of the parameter marker provided is invalid");
	}

	@Test
	void shouldThrowExceptionWhenTheParameterProvidedIsNull() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id is ?";
		Map<Integer, String> params = new HashMap<>();
		params.put(1, null);
		assertThrows(IllegalArgumentException.class, () -> StatementUtil.replaceParameterMarksWithValues(params, sql),
				"No value for parameter marker at position: 1");
	}

	@Test
	void shouldCountOnlyTwoQueriesWithTheLastQueryIsJustComments() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? ; SELECT 1;   --Some comment";
		assertEquals(2, StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldIncludeCommentsAndSemicolonsInSubQueries() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? --Fetch employee with provided id \n ;\n\n\n\n\n SELECT 1;;;;;;;;;;;;; --Some comment";
		assertEquals(
				"SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? --Fetch employee with provided id \n ;\n\n\n\n\n ",
				StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().get(0).getSql());
		assertEquals("SELECT 1;;;;;;;;;;;;; --Some comment",
				StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().get(1).getSql());
		assertEquals(2, StatementUtil.parseToSqlQueryWrapper(sql).getSubStatements().size());
	}
}
