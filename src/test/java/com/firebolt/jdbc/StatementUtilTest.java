package com.firebolt.jdbc;

import static com.firebolt.jdbc.statement.StatementType.PARAM_SETTING;
import static com.firebolt.jdbc.statement.StatementUtil.isQuery;
import static com.firebolt.jdbc.statement.StatementUtil.replaceParameterMarksWithValues;
import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.Test;

import com.firebolt.jdbc.statement.ParamMarker;
import com.firebolt.jdbc.statement.StatementUtil;
import com.firebolt.jdbc.statement.rawstatement.QueryRawStatement;
import com.firebolt.jdbc.statement.rawstatement.RawStatementWrapper;
import com.firebolt.jdbc.statement.rawstatement.SetParamRawStatement;
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
				StatementUtil.extractParamFromSetStatement(query, null));
	}

	@Test
	void shouldExtractAdditionalPropertiesWithComments() {
		String query = "/* */" + " SeT my_custom_query=1";
		String cleanQuery = "SeT my_custom_query=1";
		assertEquals(Optional.of(new ImmutablePair<>("my_custom_query", "1")),
				StatementUtil.extractParamFromSetStatement(cleanQuery, query));
	}

	@Test
	void shouldExtractAdditionalWithEmptyProperties() {
		String query = "set my_custom_char=' '";
		assertEquals(Optional.of(new ImmutablePair<>("my_custom_char", "' '")),
				StatementUtil.extractParamFromSetStatement(query, null));
	}

	@Test
	void shouldFindThatStatementsWithQueryKeywordsAreQueries() {
		List<String> keywords = Arrays.asList("shOW", "seleCt", "DESCRIBE", "exists", "explain", "with", "call");

		String query = "/* Some random command*/ -- oneLineOfComment INSERT \n %s anything";
		keywords.forEach(keyword -> assertTrue(isQuery(StatementUtil
				.parseToRawStatementWrapper(String.format(query, keyword)).getSubStatements().get(0).getCleanSql())));
	}

	@Test
	void shouldFindThatStatementWithoutQueryKeywordsAreNotQueries() {
		List<String> keywords = Arrays.asList("Insert", "updAte", "Delete", "hello");
		String query = "/* Some random command SELECT */ -- oneLineOfComment SELECT \n %s anything";
		keywords.forEach(keyword -> assertFalse(isQuery(String.format(query, keyword))));
	}

	@Test
	void shouldExtractTableNameFromQuery() {
		String query = "/* Some random comment*/ SELECT /* Second comment */ * FROM -- third comment \n EMPLOYEES WHERE id = 5";

		assertEquals("EMPLOYEES",
				((QueryRawStatement) StatementUtil.parseToRawStatementWrapper(query).getSubStatements().get(0))
						.getTable());
	}

	@Test
	void shouldExtractDbNameFromQuery() {
		String query = "-- Some random command   \n       SELECT *     FROM    db.schema.EMPLOYEES      WHERE id = 5";
		assertEquals("db",
				((QueryRawStatement) StatementUtil.parseToRawStatementWrapper(query).getSubStatements().get(0))
						.getDatabase());
	}

	@Test
	void shouldBeEmptyWhenGettingDbNameAndThereIsNoDbName() {
		String query = "/* Some random command*/ SELECT * FROM EMPLOYEES WHERE id = 5";
		assertEquals(null,
				((QueryRawStatement) StatementUtil.parseToRawStatementWrapper(query).getSubStatements().get(0))
						.getDatabase());
	}

	@Test
	void shouldBeEmptyWhenGettingDbNameFromAQueryWithoutFrom() {
		String query = "SELECT *";
		assertEquals(null,
				((QueryRawStatement) StatementUtil.parseToRawStatementWrapper(query).getSubStatements().get(0))
						.getDatabase());
	}

	@Test
	void shouldGetEmptyDbNameAndTablesTableNameWhenUsingDescribe() {
		String query = "DESCRIBE EMPLOYEES";
		assertEquals(Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromCleanQuery(query).getLeft());
		assertEquals(Optional.of("tables"),
				StatementUtil.extractDbNameAndTableNamePairFromCleanQuery(query).getRight());
	}

	@Test
	void shouldGetEmptyTableNameAndEmptyDbNameWhenUsingShow() {
		String query = "SHOW databases";
		assertEquals(Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromCleanQuery(query).getLeft());
		assertEquals(Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromCleanQuery(query).getRight());
	}

	@Test
	void shouldBeEmptyWhenGettingTableNameWhenTheQueryIsNotASelect() {
		String query = "/* Some random command*/ UPDATE * FROM EMPLOYEES WHERE id = 5";
		assertEquals(Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromCleanQuery(query).getRight());
	}

	@Test
	void shouldThrowAnExceptionWhenTheSetCannotBeParsed() {
		String query = "set x=";
		assertThrows(IllegalArgumentException.class, () -> StatementUtil.extractParamFromSetStatement(query, null));
	}

	@Test
	void shouldCleanQueryWithComments() {
		String sql = getSqlFromFile("/queries/query-with-comment.sql");
		String expectedCleanQuery = getSqlFromFile("/queries/query-with-comment-cleaned.sql");
		String cleanStatement = StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().get(0).getCleanSql();
		assertEquals(expectedCleanQuery, cleanStatement);
		assertEquals(expectedCleanQuery,
				StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().get(0).getCleanSql());
	}

	@Test
	void shouldCleanQueryWithQuotesInTheVarchar() {
		String sql = "INSERT INTO regex_test (name)\n" + "-- Hello\n" + "VALUES (/* some comment */\n"
				+ "'Taylor''s Prime Steak House 3' /* some comment */)--";
		String expectedCleanQuery = "INSERT INTO regex_test (name)\n" + "\n" + "VALUES (\n"
				+ "'Taylor''s Prime Steak House 3' )";
		assertEquals(expectedCleanQuery,
				StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().get(0).getCleanSql());

	}

	@Test
	void shouldCleanQueryWithSingleLineComment() {
		String sql = getSqlFromFile("/queries/query-with-comment.sql");
		String expectedCleanQuery = getSqlFromFile("/queries/query-with-comment-cleaned.sql");
		String cleanStatement = StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().get(0).getCleanSql();
		assertEquals(expectedCleanQuery, cleanStatement);
		assertEquals(expectedCleanQuery,
				StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().get(0).getCleanSql());

	}

	@Test
	void shouldCountParametersFromLongQueryWithComments() {
		String sql = getSqlFromFile("/queries/query-with-comment.sql");
		StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().get(0).getSql();
		assertEquals(Arrays.asList(new ParamMarker(1, 200)),
				StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().get(0).getParamMarkers());
		assertEquals(1, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParams() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ?";
		assertEquals(ImmutableMap.of(1, 35), StatementUtil.getParamMarketsPositions(sql));
		assertEquals(1, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsWithoutTrimmingRequest() {
		String sql = "     SElECT * FROM EMPLOYEES WHERE id = ?";
		assertEquals(ImmutableMap.of(1, 40), StatementUtil.getParamMarketsPositions(sql));
		assertEquals(1, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsFromIn() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN (?,?)";
		assertEquals(ImmutableMap.of(1, 37, 2, 39), StatementUtil.getParamMarketsPositions(sql));
		assertEquals(1, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInComments() {
		String sql = "SElECT * FROM EMPLOYEES WHERE /* ?*/id IN (?,?)";
		assertEquals(ImmutableMap.of(1, 43, 2, 45), StatementUtil.getParamMarketsPositions(sql));
		assertEquals(1, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInComments2() {
		String sql = "SElECT * FROM EMPLOYEES WHERE /* ?id IN (?,?)*/";
		assertEquals(ImmutableMap.of(), StatementUtil.getParamMarketsPositions(sql));
		assertEquals(1, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInSingleLineComment() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n";
		assertEquals(ImmutableMap.of(), StatementUtil.getParamMarketsPositions(sql));
		assertEquals(1, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInSingleLineComment2() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --\n(?,?)";
		assertEquals(ImmutableMap.of(1, 40, 2, 42), StatementUtil.getParamMarketsPositions(sql));
		assertEquals(1, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInBetweenQuotesOrComments() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE ? AND my_date = ?";
		assertEquals(ImmutableMap.of(1, 93, 2, 109), StatementUtil.getParamMarketsPositions(sql));
		assertEquals(1, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldReplaceOneQueryParamsThatAreNotInBetweenQuotesOrComments() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE ?";
		String expectedSql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE '55 Liverpool road%'";
		Map<Integer, String> params = ImmutableMap.of(1, "'55 Liverpool road%'");
		assertEquals(expectedSql, replaceParameterMarksWithValues(params, sql).get(0).getSql());
	}

	@Test
	void shouldReplaceAQueryParam() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id is ?";
		String expectedSql = "SElECT * FROM EMPLOYEES WHERE id is 5";
		Map<Integer, String> params = ImmutableMap.of(1, "5");
		assertEquals(expectedSql, replaceParameterMarksWithValues(params, sql).get(0).getSql());
	}

	@Test
	void shouldReplaceMultipleQueryParams() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? ";
		String expectedSql = "SElECT * FROM EMPLOYEES WHERE id = 5 AND name LIKE 'George' AND dob = '1980-05-22' ";
		Map<Integer, String> params = ImmutableMap.of(1, "5", 2, "'George'", 3, "'1980-05-22'");
		assertEquals(expectedSql, replaceParameterMarksWithValues(params, sql).get(0).getSql());
	}

	@Test
	void shouldReplaceAllQueryParamsThatAreNotInBetweenQuotesOrComments() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE ? AND my_date = ? AND age = /* */ ?";
		String expectedSql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE '55 Liverpool road%' AND my_date = '2022-01-01' AND age = /* */ 5";
		Map<Integer, String> params = ImmutableMap.of(1, "'55 Liverpool road%'", 2, "'2022-01-01'", 3, "5");
		assertEquals(expectedSql, replaceParameterMarksWithValues(params, sql).get(0).getSql());
	}

	@Test
	void shouldReplaceMultipleQueryParamsInaMultiStatement() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? ; SELECT * FROM cats WHERE name IN (?, ?)";
		String expectedFirstSql = "SElECT * FROM EMPLOYEES WHERE id = 5 AND name LIKE 'George' AND dob = '1980-05-22' ; ";
		String expectedSecondSql = "SELECT * FROM cats WHERE name IN ('Elizabeth', 'Charles')";

		Map<Integer, String> params = ImmutableMap.of(1, "5", 2, "'George'", 3, "'1980-05-22'", 4, "'Elizabeth'", 5,
				"'Charles'");
		assertEquals(expectedFirstSql, replaceParameterMarksWithValues(params, sql).get(0).getSql());
		assertEquals(expectedSecondSql, replaceParameterMarksWithValues(params, sql).get(1).getSql());
	}

	@Test
	void shouldThrowExceptionWhenTheNumberOfParamsIsNotTheSameAsTheNumberOfParamMarkers() {
		Map<Integer, String> params = ImmutableMap.of(1, "1");
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> replaceParameterMarksWithValues(params, "SELECT 1;"));
		assertEquals(
				"The number of parameters passed does not equal the number of parameter markers in the SQL query. Provided: 1, Parameter markers in the SQL query: 0",
				exception.getMessage());
	}

	@Test
	void shouldThrowExceptionWhenTheParameterProvidedIsNull() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id is ?";
		Map<Integer, String> params = new HashMap<>();
		params.put(1, null);
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> replaceParameterMarksWithValues(params, sql));
		assertEquals("No value for parameter marker at position: 1", exception.getMessage());
	}

	@Test
	void shouldCountOnlyTwoQueriesWhenTheLastPartOfTheLastQueryIsAComment() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? ; SELECT 1;   --Some comment";
		assertEquals(2, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldIncludeCommentsAndSemicolonsInSubQueries() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? --Fetch employee with provided id \n ;\n\n\n\n\n SELECT 1;;;;;;;;;;;;; --Some comment";
		assertEquals(
				"SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? --Fetch employee with provided id \n ;\n\n\n\n\n ",
				StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().get(0).getSql());
		assertEquals("SELECT 1;;;;;;;;;;;;; --Some comment",
				StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().get(1).getSql());
		assertEquals(2, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldNotConsiderCommentAsAStatementEvenWhenItEndsWithSemicolon() {
		String sql = "\n\n\n  -- PARTITION BY \"name\";\n" + "\n" + "INSERT INTO employees VALUES(\n" + "  1,\n"
				+ "  'hello',\n" + "  'world',\n" + "  'site',\n" + ");";

		assertEquals(sql, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().get(0).getSql());
		assertEquals(1, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());

	}

	@Test
	void shouldFindOnlyOneQueryAndNoParamInHugeStatement() {
		String sql = getSqlFromFile("/queries/query-with-huge-select.sql");
		assertEquals(Collections.emptyList(),
				StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().get(0).getParamMarkers());
		assertEquals(1, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
	}

	@Test
	void shouldCountOnlyTwoSubStatementsInMultiStatementWithALotOfComments() {
		String sql = "  --Getting Multiple RS;\nSELECT 1; /* comment 1 ; ; ; */\n\n --Another comment ; \n  -- ; \n SELECT 2; /* comment 2 */";
		assertEquals(2, StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().size());
		assertEquals("SELECT 1;",
				StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().get(0).getCleanSql());
		assertEquals("SELECT 2;",
				StatementUtil.parseToRawStatementWrapper(sql).getSubStatements().get(1).getCleanSql());
	}

	@Test
	void shouldNotConsiderStatementCleanIfEmpty() {
		assertFalse(isQuery(""));
	}

	@Test
	void shouldThrowExceptionWhenTryingToSetParamsWithNullParamsMap() {
		assertThrows(NullPointerException.class, () -> replaceParameterMarksWithValues(null, "SELECT 1;"));
	}

	@Test
	void shouldThrowExceptionWhenTryingToSetAParamWithAnInvalidPosition() {
		Map<Integer, String> params = new HashMap<>();
		params.put(2, "John");
		assertThrows(IllegalArgumentException.class,
				() -> replaceParameterMarksWithValues(params, "SELECT * FROM employees WHERE name = ?"));
	}

	@Test
	void shouldParseStatementWithTypeParamSettingForStatementThatSetAdditionalParam() {
		String sql = "-- comment \n \n    SET x = 1 ;";
		RawStatementWrapper rawStatementWrapper = StatementUtil.parseToRawStatementWrapper(sql);
		assertEquals(PARAM_SETTING, rawStatementWrapper.getSubStatements().get(0).getStatementType());
		assertEquals(ImmutablePair.of("x", "1"),
				((SetParamRawStatement) rawStatementWrapper.getSubStatements().get(0)).getAdditionalProperty());
	}

}
