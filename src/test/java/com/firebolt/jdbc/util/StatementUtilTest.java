package com.firebolt.jdbc.util;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.statement.ParamMarker;
import com.firebolt.jdbc.statement.StatementUtil;
import com.firebolt.jdbc.statement.preparedstatement.PreparedStatementParamStyle;
import com.firebolt.jdbc.statement.rawstatement.QueryRawStatement;
import com.firebolt.jdbc.statement.rawstatement.RawStatement;
import com.firebolt.jdbc.statement.rawstatement.RawStatementWrapper;
import com.firebolt.jdbc.statement.rawstatement.SetParamRawStatement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.firebolt.jdbc.statement.StatementType.PARAM_SETTING;
import static com.firebolt.jdbc.statement.StatementUtil.isQuery;
import static com.firebolt.jdbc.statement.StatementUtil.parseToRawStatementWrapperNative;
import static com.firebolt.jdbc.statement.StatementUtil.replaceParameterMarksWithValues;
import static com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.createValidator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StatementUtilTest {

	private static String getSqlFromFile(String path) {
		InputStream is = StatementUtilTest.class.getResourceAsStream(path);
		return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)).lines()
				.collect(Collectors.joining("\n"));
	}

	@Test
	void shouldExtractAdditionalProperties() {
		String query = "set my_custom_query=1";
		assertEquals(Optional.of(Map.entry("my_custom_query", "1")),
				StatementUtil.extractParamFromSetStatement(query, null));
	}

	@Test
	void shouldExtractAdditionalPropertiesWithComments() {
		String query = "/* */" + " SeT my_custom_query=1";
		String cleanQuery = "SeT my_custom_query=1";
		assertEquals(Optional.of(Map.entry("my_custom_query", "1")),
				StatementUtil.extractParamFromSetStatement(cleanQuery, query));
	}

	@Test
	void shouldExtractAdditionalWithEmptyProperties() {
		String query = "set my_custom_char=' '";
		assertEquals(Optional.of(Map.entry("my_custom_char", " ")),
				StatementUtil.extractParamFromSetStatement(query, null));
	}
	@Test
	void shouldExtractTimezone() {
		String query = "set time_zone='Europe/Berlin';";
		assertEquals(Optional.of(Map.entry("time_zone", "Europe/Berlin")),
				StatementUtil.extractParamFromSetStatement(query, null));
	}

	@Test
	void shouldReturnEmptyListForEmptySql() {
		assertTrue(parseToRawStatementWrapperNative("").getSubStatements().isEmpty());
	}

	@ParameterizedTest
	@CsvSource(value = {
			"1,set database='my_db',Could not set parameter. Set parameter 'DATABASE' is not allowed. Try again with 'USE DATABASE' instead of SET.",
			"1,set engine='my_engine',Could not set parameter. Set parameter 'ENGINE' is not allowed. Try again with 'USE ENGINE' instead of SET.",
			"1,set account_id='123',Could not set parameter. Set parameter 'ACCOUNT_ID' is not allowed. Try again with a different parameter name.",
			"1,set output_format='XML',Could not set parameter. Set parameter 'OUTPUT_FORMAT' is not allowed. Try again with a different parameter name.",
			"2,set database='my_db',Could not set parameter. Set parameter 'DATABASE' is not allowed. Try again with 'USE DATABASE' instead of SET.",
			"2,set engine='my_engine',Could not set parameter. Set parameter 'ENGINE' is not allowed. Try again with 'USE ENGINE' instead of SET.",
			"2,set output_format='XML',Could not set parameter. Set parameter 'OUTPUT_FORMAT' is not allowed. Try again with a different parameter name."
	})
	void shouldThrowWhenSettingPropertyThatShouldBeSetByUse(int infraVersion, String statement, String error) {
		FireboltConnection connection = mock(FireboltConnection.class);
		when(connection.getInfraVersion()).thenReturn(infraVersion);
		List<RawStatement> rawStatements = parseToRawStatementWrapperNative(statement).getSubStatements();
		IllegalArgumentException e = assertThrows(
				IllegalArgumentException.class,
				() -> rawStatements.forEach(s -> createValidator(s, connection).validate(s)));
		assertEquals(error, e.getMessage());
	}

	@Test
	void shouldSuccessfullySetAccountIdForInfraVersion2() {
		FireboltConnection connection = mock(FireboltConnection.class);
		when(connection.getInfraVersion()).thenReturn(2);
		parseToRawStatementWrapperNative("set account_id='123'").getSubStatements().forEach(s -> createValidator(s, connection).validate(s));
	}

	@Test
	void shouldFindThatStatementsWithQueryKeywordsAreQueries() {
		List<String> keywords = Arrays.asList("shOW", "seleCt", "DESCRIBE", "exists", "explain", "with", "call");

		String query = "/* Some random command*/ -- oneLineOfComment INSERT \n %s anything";
		keywords.forEach(keyword -> assertTrue(isQuery(parseToRawStatementWrapperNative(String.format(query, keyword)).getSubStatements().get(0).getCleanSql())));
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
				((QueryRawStatement) parseToRawStatementWrapperNative(query).getSubStatements().get(0))
						.getTable());
	}

	@Test
	void shouldExtractDbNameFromQuery() {
		String query = "-- Some random command   \n       SELECT *     FROM    db.schema.EMPLOYEES      WHERE id = 5";
		assertEquals("db",
				((QueryRawStatement) parseToRawStatementWrapperNative(query).getSubStatements().get(0))
						.getDatabase());
	}

	@Test
	void shouldBeEmptyWhenGettingDbNameAndThereIsNoDbName() {
		String query = "/* Some random command*/ SELECT * FROM EMPLOYEES WHERE id = 5";
        assertNull(((QueryRawStatement) parseToRawStatementWrapperNative(query).getSubStatements().get(0))
                .getDatabase());
	}

	@Test
	void shouldBeEmptyWhenGettingDbNameFromAQueryWithoutFrom() {
		String query = "SELECT *";
        assertNull(((QueryRawStatement) parseToRawStatementWrapperNative(query).getSubStatements().get(0))
                .getDatabase());
	}

	@Test
	void shouldGetEmptyDbNameAndTablesTableNameWhenUsingDescribe() {
		String query = "DESCRIBE EMPLOYEES";
		assertEquals(Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromCleanQuery(query).getKey());
		assertEquals(Optional.of("tables"),
				StatementUtil.extractDbNameAndTableNamePairFromCleanQuery(query).getValue());
	}

	@Test
	void shouldGetEmptyTableNameAndEmptyDbNameWhenUsingShow() {
		String query = "SHOW databases";
		assertEquals(Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromCleanQuery(query).getKey());
		assertEquals(Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromCleanQuery(query).getValue());
	}

	@Test
	void shouldBeEmptyWhenGettingTableNameWhenTheQueryIsNotASelect() {
		String query = "/* Some random command*/ UPDATE * FROM EMPLOYEES WHERE id = 5";
		assertEquals(Optional.empty(), StatementUtil.extractDbNameAndTableNamePairFromCleanQuery(query).getValue());
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
		String cleanStatement = parseToRawStatementWrapperNative(sql).getSubStatements().get(0).getCleanSql();
		assertEquals(expectedCleanQuery, cleanStatement);
		assertEquals(expectedCleanQuery,
				parseToRawStatementWrapperNative(sql).getSubStatements().get(0).getCleanSql());
	}

	@Test
	void shouldCleanQueryWithQuotesInTheVarchar() {
		String sql = "INSERT INTO regex_test (name)\n" + "-- Hello\n" + "VALUES (/* some comment */\n"
				+ "'Taylor''s Prime Steak House 3' /* some comment */)--";
		String expectedCleanQuery = "INSERT INTO regex_test (name)\n" + "\n" + "VALUES (\n"
				+ "'Taylor''s Prime Steak House 3' )";
		assertEquals(expectedCleanQuery,
				parseToRawStatementWrapperNative(sql).getSubStatements().get(0).getCleanSql());

	}

	@Test
	void shouldCleanQueryWithSingleLineComment() {
		String sql = getSqlFromFile("/queries/query-with-comment.sql");
		String expectedCleanQuery = getSqlFromFile("/queries/query-with-comment-cleaned.sql");
		String cleanStatement = parseToRawStatementWrapperNative(sql).getSubStatements().get(0).getCleanSql();
		assertEquals(expectedCleanQuery, cleanStatement);
		assertEquals(expectedCleanQuery,
				parseToRawStatementWrapperNative(sql).getSubStatements().get(0).getCleanSql());

	}

	@Test
	void shouldCountParametersFromLongQueryWithComments() {
		String sql = getSqlFromFile("/queries/query-with-comment.sql");
		parseToRawStatementWrapperNative(sql).getSubStatements().get(0).getSql();
		assertEquals(List.of(new ParamMarker(1, 200)),
				parseToRawStatementWrapperNative(sql).getSubStatements().get(0).getParamMarkers());
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsNative() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ?";
		assertEquals(Map.of(1, 35), StatementUtil.getParamMarketsPositionsNative(sql));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsFbNumeric() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = $1";
		assertEquals(Map.of(1, 35), StatementUtil.getParamMarketsPositions(sql, PreparedStatementParamStyle.FB_NUMERIC));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsWithoutTrimmingRequestNative() {
		String sql = "     SElECT * FROM EMPLOYEES WHERE id = ?";
		assertEquals(Map.of(1, 40), StatementUtil.getParamMarketsPositionsNative(sql));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsWithoutTrimmingRequestFbNumeric() {
		String sql = "     SElECT * FROM EMPLOYEES WHERE id = $1";
		assertEquals(Map.of(1, 40), StatementUtil.getParamMarketsPositions(sql, PreparedStatementParamStyle.FB_NUMERIC));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsFromInNative() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN (?,?)";
		assertEquals(Map.of(1, 37, 2, 39), StatementUtil.getParamMarketsPositionsNative(sql));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsFromInFbNumeric() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN ($1,$2)";
		assertEquals(Map.of(1, 37, 2, 40), StatementUtil.getParamMarketsPositions(sql, PreparedStatementParamStyle.FB_NUMERIC));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInCommentsNative() {
		String sql = "SElECT * FROM EMPLOYEES WHERE /* ?*/id IN (?,?)";
		assertEquals(Map.of(1, 43, 2, 45), StatementUtil.getParamMarketsPositionsNative(sql));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInCommentsFbNumeric() {
		String sql = "SElECT * FROM EMPLOYEES WHERE /* ?*/id IN ($1,$2)";
		assertEquals(Map.of(1, 43, 2, 46), StatementUtil.getParamMarketsPositions(sql, PreparedStatementParamStyle.FB_NUMERIC));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInComments2Native() {
		String sql = "SElECT * FROM EMPLOYEES WHERE /* ?id IN (?,?)*/";
		assertEquals(Map.of(), StatementUtil.getParamMarketsPositionsNative(sql));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInComments2FbNumeric() {
		String sql = "SElECT * FROM EMPLOYEES WHERE /* ?id IN ($1,$2)*/";
		assertEquals(Map.of(), StatementUtil.getParamMarketsPositions(sql, PreparedStatementParamStyle.FB_NUMERIC));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInSingleLineCommentNative() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n";
		assertEquals(Map.of(), StatementUtil.getParamMarketsPositionsNative(sql));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInSingleLineCommentFbNumeric() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --($1,$2)\n";
		assertEquals(Map.of(), StatementUtil.getParamMarketsPositions(sql, PreparedStatementParamStyle.FB_NUMERIC));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInSingleLineCommentNative2() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --\n(?,?)";
		assertEquals(Map.of(1, 40, 2, 42), StatementUtil.getParamMarketsPositionsNative(sql));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInSingleLineCommentFbNumeric2() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --\n($1,$2)";
		assertEquals(Map.of(1, 40, 2, 43), StatementUtil.getParamMarketsPositions(sql, PreparedStatementParamStyle.FB_NUMERIC));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInBetweenQuotesOrCommentsNative() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE ? AND my_date = ?";
		assertEquals(Map.of(1, 93, 2, 109), StatementUtil.getParamMarketsPositionsNative(sql));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldGetAllQueryParamsThatAreNotInBetweenQuotesOrCommentsFbNumeric() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --($1,$2)\n AND name NOT LIKE '$1 Hello $2 ' AND address LIKE $1 AND my_date = $2";
		assertEquals(Map.of(1, 97, 2, 114), StatementUtil.getParamMarketsPositions(sql, PreparedStatementParamStyle.FB_NUMERIC));
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldReplaceOneQueryParamsThatAreNotInBetweenQuotesOrComments() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE ?";
		String expectedSql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE '55 Liverpool road%'";
		Map<Integer, Object> params = Map.of(1, "'55 Liverpool road%'");
		assertEquals(expectedSql, replaceParameterMarksWithValues(params, sql).get(0).getSql());
	}

	@Test
	void shouldReplaceAQueryParam() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id is ?";
		String expectedSql = "SElECT * FROM EMPLOYEES WHERE id is 5";
		Map<Integer, Object> params = Map.of(1, "5");
		assertEquals(expectedSql, replaceParameterMarksWithValues(params, sql).get(0).getSql());
	}

	@Test
	void shouldReplaceMultipleQueryParams() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? ";
		String expectedSql = "SElECT * FROM EMPLOYEES WHERE id = 5 AND name LIKE 'George' AND dob = '1980-05-22' ";
		Map<Integer, Object> params = Map.of(1, "5", 2, "'George'", 3, "'1980-05-22'");
		assertEquals(expectedSql, replaceParameterMarksWithValues(params, sql).get(0).getSql());
	}

	@Test
	void shouldReplaceAllQueryParamsThatAreNotInBetweenQuotesOrComments() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE ? AND my_date = ? AND age = /* */ ?";
		String expectedSql = "SElECT * FROM EMPLOYEES WHERE id IN --(?,?)\n AND name NOT LIKE '? Hello ? ' AND address LIKE '55 Liverpool road%' AND my_date = '2022-01-01' AND age = /* */ 5";
		Map<Integer, Object> params = Map.of(1, "'55 Liverpool road%'", 2, "'2022-01-01'", 3, "5");
		assertEquals(expectedSql, replaceParameterMarksWithValues(params, sql).get(0).getSql());
	}

	@Test
	void shouldReplaceMultipleQueryParamsInaMultiStatement() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? ; SELECT * FROM cats WHERE name IN (?, ?)";
		String expectedFirstSql = "SElECT * FROM EMPLOYEES WHERE id = 5 AND name LIKE 'George' AND dob = '1980-05-22' ; ";
		String expectedSecondSql = "SELECT * FROM cats WHERE name IN ('Elizabeth', 'Charles')";

		Map<Integer, Object> params = Map.of(1, "5", 2, "'George'", 3, "'1980-05-22'", 4, "'Elizabeth'", 5,
				"'Charles'");
		assertEquals(expectedFirstSql, replaceParameterMarksWithValues(params, sql).get(0).getSql());
		assertEquals(expectedSecondSql, replaceParameterMarksWithValues(params, sql).get(1).getSql());
	}

	@Test
	void shouldThrowExceptionWhenTheNumberOfParamsIsNotTheSameAsTheNumberOfParamMarkers() {
		Map<Integer, Object> params = Map.of(1, "1");
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> replaceParameterMarksWithValues(params, "SELECT 1;"));
		assertEquals(
				"The number of parameters passed does not equal the number of parameter markers in the SQL query. Provided: 1, Parameter markers in the SQL query: 0",
				exception.getMessage());
	}

	@Test
	void shouldThrowExceptionWhenTheParameterProvidedIsNull() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id is ?";
		Map<Integer, Object> params = new HashMap<>();
		params.put(1, null);
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
				() -> replaceParameterMarksWithValues(params, sql));
		assertEquals("No value for parameter marker at position: 1", exception.getMessage());
	}

	@Test
	void shouldCountOnlyTwoQueriesWhenTheLastPartOfTheLastQueryIsAComment() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? ; SELECT 1;   --Some comment";
		assertEquals(2, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldIncludeCommentsAndSemicolonsInSubQueries() {
		String sql = "SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? --Fetch employee with provided id \n ;\n\n\n\n\n SELECT 1;;;;;;;;;;;;; --Some comment";
		assertEquals(
				"SElECT * FROM EMPLOYEES WHERE id = ? AND name LIKE ? AND dob = ? --Fetch employee with provided id \n ;\n\n\n\n\n ",
				parseToRawStatementWrapperNative(sql).getSubStatements().get(0).getSql());
		assertEquals("SELECT 1;;;;;;;;;;;;; --Some comment",
				parseToRawStatementWrapperNative(sql).getSubStatements().get(1).getSql());
		assertEquals(2, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldNotConsiderCommentAsAStatementEvenWhenItEndsWithSemicolon() {
		String sql = "\n\n\n  -- PARTITION BY \"name\";\n" + "\n" + "INSERT INTO employees VALUES(\n" + "  1,\n"
				+ "  'hello',\n" + "  'world',\n" + "  'site',\n" + ");";

		assertEquals(sql, parseToRawStatementWrapperNative(sql).getSubStatements().get(0).getSql());
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());

	}

	@Test
	void shouldFindOnlyOneQueryAndNoParamInHugeStatement() {
		String sql = getSqlFromFile("/queries/query-with-huge-select.sql");
		assertEquals(Collections.emptyList(),
				parseToRawStatementWrapperNative(sql).getSubStatements().get(0).getParamMarkers());
		assertEquals(1, parseToRawStatementWrapperNative(sql).getSubStatements().size());
	}

	@Test
	void shouldCountOnlyTwoSubStatementsInMultiStatementWithALotOfComments() {
		String sql = "  --Getting Multiple RS;\nSELECT 1; /* comment 1 ; ; ; */\n\n --Another comment ; \n  -- ; \n SELECT 2; /* comment 2 */";
		assertEquals(2, parseToRawStatementWrapperNative(sql).getSubStatements().size());
		assertEquals("SELECT 1;",
				parseToRawStatementWrapperNative(sql).getSubStatements().get(0).getCleanSql());
		assertEquals("SELECT 2;",
				parseToRawStatementWrapperNative(sql).getSubStatements().get(1).getCleanSql());
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
		Map<Integer, Object> params = new HashMap<>();
		params.put(2, "John");
		assertThrows(IllegalArgumentException.class,
				() -> replaceParameterMarksWithValues(params, "SELECT * FROM employees WHERE name = ?"));
	}

	@Test
	void shouldParseStatementWithTypeParamSettingForStatementThatSetAdditionalParam() {
		String sql = "-- comment \n \n    SET x = 1 ;";
		RawStatementWrapper rawStatementWrapper = parseToRawStatementWrapperNative(sql);
		assertEquals(PARAM_SETTING, rawStatementWrapper.getSubStatements().get(0).getStatementType());
		assertEquals(Map.entry("x", "1"),
				((SetParamRawStatement) rawStatementWrapper.getSubStatements().get(0)).getAdditionalProperty());
	}

	@Test
	void shouldParseStatementWithQuestionMarksBetweenDoubleQuotes() {
		String sql = "SELECT \"source\".\"id\"                AS \"id\",\n"
				+ "       \"source\".\"name\"              AS \"name\",\n"
				+ "       \"source\".\"category_id\"       AS \"category_id\",\n"
				+ "       \"source\".\"latitude\"          AS \"latitude\",\n"
				+ "       \"source\".\"longitude\"         AS \"longitude\",\n"
				+ "       \"source\".\"price\"             AS \"price\",\n"
				+ "       \"source\".\"Refund Amount (?)\" AS \"Refund Amount (?)\"\n"
				+ "FROM (SELECT \"test_data_venues\".\"id\"           AS \"id\",\n"
				+ "             \"test_data_venues\".\"name\"         AS \"name\",\n"
				+ "             \"test_data_venues\".\"category_id\"  AS \"category_id\",\n"
				+ "             \"test_data_venues\".\"latitude\"     AS \"latitude\",\n"
				+ "             \"test_data_venues\".\"longitude\"    AS \"longitude\",\n"
				+ "             \"test_data_venues\".\"price\"        AS \"price\",\n"
				+ "             (\"test_data_venues\".\"price\" * -1) AS \"Refund Amount (?)\"\n"
				+ "      FROM \"test_data_venues\") \"source\"\n" + "ORDER BY \"source\".\"id\" ASC LIMIT 1";
		RawStatementWrapper rawStatementWrapper = parseToRawStatementWrapperNative(sql);
		assertEquals(0, rawStatementWrapper.getTotalParams());
	}

}
