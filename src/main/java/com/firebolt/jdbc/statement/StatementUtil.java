package com.firebolt.jdbc.statement;

import com.firebolt.jdbc.statement.preparedstatement.PreparedStatementParamStyle;
import com.firebolt.jdbc.statement.rawstatement.RawStatement;
import com.firebolt.jdbc.statement.rawstatement.RawStatementWrapper;
import com.firebolt.jdbc.statement.rawstatement.SetParamRawStatement;
import com.firebolt.jdbc.util.StringUtil;
import lombok.CustomLog;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@UtilityClass
@CustomLog
public class StatementUtil {

	private static final String SET_PREFIX = "set";
	private static final Pattern SET_WITH_SPACE_REGEX = Pattern.compile(SET_PREFIX + " ", Pattern.CASE_INSENSITIVE);
	private static final Pattern FB_NUMERIC_PARAMETER = Pattern.compile("\\$(\\d+)");
	private static final String[] SELECT_KEYWORDS = new String[] { "show", "select", "describe", "exists", "explain",
			"with", "call" };

	/**
	 * Returns true if the statement is a query (eg: SELECT, SHOW).
	 * 
	 * @param cleanSql the clean sql (sql statement without comments)
	 * @return true if the statement is a query (eg: SELECT, SHOW).
	 */
	public static boolean isQuery(String cleanSql) {
		if (cleanSql == null || cleanSql.isEmpty()) {
			return false;
		}
		String lowerCaseSql = cleanSql.replace("(", "").toLowerCase();
		return Arrays.stream(SELECT_KEYWORDS).anyMatch(lowerCaseSql::startsWith);
	}

	/**
	 * Extracts parameter from statement (eg: SET x=y)
	 * 
	 * @param cleanSql the clean version of the sql (sql statement without comments)
	 * @param sql      the sql statement
	 * @return an optional parameter represented with a pair of key/value
	 */
	public Optional<Entry<String, String>> extractParamFromSetStatement(@NonNull String cleanSql, String sql) {
		return cleanSql.toLowerCase().startsWith(SET_PREFIX) ? extractPropertyPair(cleanSql, sql) : Optional.empty();
	}

	/**
	 * Parse the sql statement to a list of {@link StatementInfoWrapper}
	 * 
	 * @param sql the sql statement
	 * @return a list of {@link StatementInfoWrapper}
	 */
	public List<StatementInfoWrapper> parseToStatementInfoWrappers(String sql) {
		return parseToRawStatementWrapperNative(sql).getSubStatements().stream().map(StatementInfoWrapper::of)
				.collect(Collectors.toList());
	}

	/**
	 * Parse sql statement to a {@link RawStatementWrapper}. The method construct
	 * the {@link RawStatementWrapper} by splitting it in a list of sub-statements
	 * (supports multi-statements)
	 * 
	 * @param sql the sql statement
	 * @return a list of {@link StatementInfoWrapper}
	 */
	public RawStatementWrapper parseToRawStatementWrapperNative(String sql) {
		return parseToRawStatementWrapper(sql, PreparedStatementParamStyle.NATIVE);
	}

	public RawStatementWrapper parseToRawStatementWrapper(String sql, PreparedStatementParamStyle paramStyle) {
		if (sql.isEmpty()) {
			return new RawStatementWrapper(List.of());
		}
		return new SqlParser(sql, paramStyle).parse();
	}

	private boolean isEndingSemicolon(char currentChar, char previousChar, boolean foundSubQueryEndingSemicolon,
			boolean isPreviousCharInComment) {
		if (foundSubQueryEndingSemicolon) {
			return true;
		}
		return (';' == previousChar && currentChar != ';' && !isPreviousCharInComment);
	}

	private boolean isEndOfSubQuery(char currentChar) {
		return currentChar != '-' && currentChar != '/' && currentChar != ' ' && currentChar != '\n';
	}

	private boolean isCommentStart(char currentChar) {
		return currentChar == '-' || currentChar == '/';
	}

	private static boolean isInMultipleLinesComment(char currentChar, char previousChar,
			boolean isCurrentSubstringBetweenQuotes, boolean isInMultipleLinesComment) {
		if (!isCurrentSubstringBetweenQuotes && (previousChar == '/' && currentChar == '*')) {
			return true;
		} else if ((previousChar == '*' && currentChar == '/')) {
			return false;
		}
		return isInMultipleLinesComment;
	}

	/**
	 * Returns the positions of the params markers
	 * 
	 * @param sql the sql statement
	 * @return the positions of the params markers
	 */
	public Map<Integer, Integer> getParamMarketsPositions(String sql, PreparedStatementParamStyle paramStyle) {
		RawStatementWrapper rawStatementWrapper = parseToRawStatementWrapper(sql, paramStyle);
		return rawStatementWrapper.getSubStatements().stream().map(RawStatement::getParamMarkers)
				.flatMap(Collection::stream).collect(Collectors.toMap(ParamMarker::getId, ParamMarker::getPosition));
	}

	public Map<Integer, Integer> getParamMarketsPositionsNative(String sql) {
		return getParamMarketsPositions(sql, PreparedStatementParamStyle.NATIVE);
	}

	/**
	 * Extract the database name and the table name from the cleaned sql query
	 * 
	 * @param cleanSql the clean sql query
	 * @return the database name and the table name from the sql query as a pair
	 */
	public Entry<Optional<String>, Optional<String>> extractDbNameAndTableNamePairFromCleanQuery(String cleanSql) {
		Optional<String> from = Optional.empty();
		if (isQuery(cleanSql)) {
			log.debug("Extracting DB and Table name for SELECT: {}", cleanSql);
			String withoutQuotes = cleanSql.replace("'", "").trim();
			String withoutQuotesUpperCase = withoutQuotes.toUpperCase();
			if (withoutQuotesUpperCase.startsWith("SELECT")) {
				int fromIndex = withoutQuotesUpperCase.indexOf("FROM");
				if (fromIndex != -1) {
					from = Optional.of(withoutQuotes.substring(fromIndex + "FROM".length()).trim().split(" ")[0]);
				}
			} else if (withoutQuotesUpperCase.startsWith("DESCRIBE")) {
				from = Optional.of("tables");
			} else if (withoutQuotesUpperCase.startsWith("SHOW")) {
				from = Optional.empty(); // Depends on the information requested
			} else {
				log.debug("Could not find table name for query {}. This may happen when there is no table.", cleanSql);
			}
		}
		return Map.entry(extractDbNameFromFromPartOfTheQuery(from.orElse(null)),
				extractTableNameFromFromPartOfTheQuery(from.orElse(null)));
	}

	/**
	 * Returns a list of {@link StatementInfoWrapper} containing sql statements
	 * constructed with the sql statement and the parameters provided
	 * 
	 * @param params the parameters
	 * @param sql    the sql statement
	 * @return a list of sql statements containing the provided parameters
	 */
	public static List<StatementInfoWrapper> replaceParameterMarksWithValues(@NonNull Map<Integer, Object> params,
			@NonNull String sql) {
		RawStatementWrapper rawStatementWrapper = parseToRawStatementWrapperNative(sql);
		return replaceParameterMarksWithValues(params, rawStatementWrapper);
	}

	/**
	 * Returns a list of {@link StatementInfoWrapper} containing sql statements
	 * constructed with the {@link RawStatementWrapper} and the parameters provided
	 * 
	 * @param params       the parameters
	 * @param rawStatement the rawStatement
	 * @return a list of sql statements containing the provided parameters
	 */
	public List<StatementInfoWrapper> replaceParameterMarksWithValues(@NonNull Map<Integer, Object> params,
			@NonNull RawStatementWrapper rawStatement) {
		List<StatementInfoWrapper> subQueries = new ArrayList<>();
		for (int subQueryIndex = 0; subQueryIndex < rawStatement.getSubStatements().size(); subQueryIndex++) {
			int currentPos;
			/*
			 * As the parameter markers are being placed then the statement sql keeps
			 * getting bigger, which is why we need to keep track of the offset
			 */
			int offset = 0;
			RawStatement subQuery = rawStatement.getSubStatements().get(subQueryIndex);
			String subQueryWithParams = subQuery.getSql();

			if (params.size() != rawStatement.getTotalParams()) {
				throw new IllegalArgumentException(String.format(
						"The number of parameters passed does not equal the number of parameter markers in the SQL query. Provided: %d, Parameter markers in the SQL query: %d",
						params.size(), rawStatement.getTotalParams()));
			}
			for (ParamMarker param : subQuery.getParamMarkers()) {
				String value = (String) params.get(param.getId());
				if (value == null) {
					throw new IllegalArgumentException("No value for parameter marker at position: " + param.getId());
				}
				currentPos = param.getPosition() + offset;
				if (currentPos >= subQuery.getSql().length() + offset) {
					throw new IllegalArgumentException("The position of the parameter marker provided is invalid");
				}
				subQueryWithParams = subQueryWithParams.substring(0, currentPos) + value
						+ subQueryWithParams.substring(currentPos + 1);
				offset += value.length() - 1;
			}
			Entry<String, String> additionalParams = subQuery.getStatementType() == StatementType.PARAM_SETTING
					? ((SetParamRawStatement) subQuery).getAdditionalProperty()
					: null;
			subQueries.add(new StatementInfoWrapper(subQueryWithParams, subQuery.getStatementType(), additionalParams, subQuery, null));
		}
		return subQueries;
	}

	/**
	 * Returns a list of {@link StatementInfoWrapper} containing sql statements
	 * from the {@link RawStatementWrapper} with the parameters provided in the {@link StatementInfoWrapper} queryParameters String
	 *
	 * @param params       the parameters
	 * @param rawStatement the rawStatement
	 * @return a list of sql statements containing the provided parameters inside the queryParameters String
	 */
	public static List<StatementInfoWrapper> prepareFbNumericStatement(@NonNull Map<Integer, Object> params,
			@NonNull RawStatementWrapper rawStatement) {
		List<StatementInfoWrapper> subQueries = new ArrayList<>();
		String queryParameters = getPreparedStatementQueryParameters(params);
		for (int subQueryIndex = 0; subQueryIndex < rawStatement.getSubStatements().size(); subQueryIndex++) {
			RawStatement subQuery = rawStatement.getSubStatements().get(subQueryIndex);

			Entry<String, String> additionalParams = subQuery.getStatementType() == StatementType.PARAM_SETTING
					? ((SetParamRawStatement) subQuery).getAdditionalProperty()
					: null;
			subQueries.add(new StatementInfoWrapper(subQuery.getSql(), subQuery.getStatementType(), additionalParams, subQuery, queryParameters));
		}
		return subQueries;
	}

	private String getPreparedStatementQueryParameters(@NonNull Map<Integer, Object> params) {
		JSONArray jsonArray = new JSONArray();
		params.forEach((key, value) -> jsonArray.put(new JSONObject().put("name", "$" + key).put("value", value == null ? JSONObject.NULL : value)));

		return jsonArray.toString();
	}

	private Optional<String> extractTableNameFromFromPartOfTheQuery(String from) {
		return Optional.ofNullable(from).map(s -> s.replace("\"", "")).map(fromPartOfTheQuery -> {
			int indexOfTableName = fromPartOfTheQuery.lastIndexOf('.');
			return indexOfTableName >= 0 && indexOfTableName < fromPartOfTheQuery.length() - 1 ? fromPartOfTheQuery.substring(indexOfTableName + 1) : fromPartOfTheQuery;
		});
	}

	private static Optional<String> extractDbNameFromFromPartOfTheQuery(String from) {
		return Optional.ofNullable(from).map(s -> s.replace("\"", ""))
				.filter(s -> s.chars().filter(c -> '.' == c).count() == 2).map(fromPartOfTheQuery -> {
					int dbNameEndPos = fromPartOfTheQuery.indexOf('.');
					return fromPartOfTheQuery.substring(0, dbNameEndPos);
				});
	}

	private boolean isInSingleLineComment(char currentChar, char previousChar, boolean isCurrentSubstringBetweenQuotes,
			boolean isInSingleLineComment) {
		if (!isCurrentSubstringBetweenQuotes && (previousChar == '-' && currentChar == '-')) {
			return true;
		} else if (currentChar == '\n') {
			return false;
		}
		return isInSingleLineComment;
	}

	private Optional<Entry<String, String>> extractPropertyPair(String cleanStatement, String sql) {
		String setQuery = SET_WITH_SPACE_REGEX.matcher(cleanStatement).replaceFirst("");
		String[] values = setQuery.split("=");
		if (values.length == 2) {
			String value = (values[1].endsWith(";") ? values[1].substring(0, values[1].length() - 1) : values[1]).trim();
			String pureValue = value.chars().allMatch(Character::isDigit) ? value : StringUtil.strip(value, '\'');
			return Optional.of(Map.entry(values[0].trim(), pureValue));
		}
		throw new IllegalArgumentException("Cannot parse the additional properties provided in the statement: " + sql);
	}

	private static class SqlParser {
		private final String sql;
		private final PreparedStatementParamStyle paramStyle;
		private final char queryParamChar;

		private int currentIndex = 0;
		private int subQueryStart = 0;
		private int subQueryParamsCount = 0;

		private boolean isInSingleQuote = false;
		private boolean isInDoubleQuote = false;
		private boolean isInSingleLineComment = false;
		private boolean isInMultiLineComment = false;
		private boolean foundSubQuerySemicolon = false;

		private final List<RawStatement> subStatements = new ArrayList<>();
		private final StringBuilder cleanedSubQuery = new StringBuilder();
		private List<ParamMarker> currentParamMarkers = new ArrayList<>();

		public SqlParser(String sql, PreparedStatementParamStyle paramStyle) {
			this.sql = sql;
			this.paramStyle = paramStyle;
			this.queryParamChar = paramStyle.getQueryParam();
		}

		public RawStatementWrapper parse() {
			char currentChar = sql.charAt(currentIndex);
			if (!isCommentStart(currentChar)) {
				cleanedSubQuery.append(currentChar);
			}

			while (currentIndex++ < sql.length() - 1) {
				char previousChar = sql.charAt(currentIndex - 1);
				currentChar = sql.charAt(currentIndex);

				boolean wasInComment = isInComment();
				updateCommentState(currentChar, previousChar);

				if (!isInComment()) {
					if (!isInSingleQuote && isEndingSemicolon(currentChar, previousChar, foundSubQuerySemicolon, wasInComment)) {
						handleEndOfSubQuery(currentChar);
					} else {
						handleCommentOrParamMarker(currentChar);
					}
					if (!(isCommentStart(currentChar) && !isInSingleQuote)) {
						cleanedSubQuery.append(currentChar);
					}
				}

			}

			// Final sub-statement
			finalizeSubStatement();
			return new RawStatementWrapper(subStatements);
		}

		private void handleEndOfSubQuery(char currentChar) {
			foundSubQuerySemicolon = true;
			if (isEndOfSubQuery(currentChar)) {
				finalizeSubStatement();
				resetSubQueryState();
			}
		}

		private void finalizeSubStatement() {
			String originalSql = sql.substring(subQueryStart, currentIndex);
			String cleaned = cleanedSubQuery.toString().trim();
			subStatements.add(RawStatement.of(originalSql, currentParamMarkers, cleaned));
		}

		private void resetSubQueryState() {
			subQueryStart = currentIndex;
			currentParamMarkers = new ArrayList<>();
			foundSubQuerySemicolon = false;
			cleanedSubQuery.setLength(0);
		}

		private void handleCommentOrParamMarker(char currentChar) {
			if (currentChar == '\'') {
				isInSingleQuote = !isInSingleQuote;
			} else if (currentChar == '"') {
				isInDoubleQuote = !isInDoubleQuote;
			} else if (currentChar == queryParamChar && !isInSingleQuote && !isInDoubleQuote) {
				if (paramStyle == PreparedStatementParamStyle.NATIVE) {
					currentParamMarkers.add(new ParamMarker(++subQueryParamsCount, currentIndex - subQueryStart));
				} else if (paramStyle == PreparedStatementParamStyle.FB_NUMERIC) {
					Matcher matcher = FB_NUMERIC_PARAMETER.matcher(sql.substring(currentIndex));
					if (matcher.find()) {
						currentParamMarkers.add(new ParamMarker(Integer.parseInt(matcher.group(1)), currentIndex - subQueryStart));
					}
				}
			}
		}

		private void updateCommentState(char current, char previous) {
			isInSingleLineComment = isInSingleLineComment(current, previous, isInSingleQuote, isInSingleLineComment);
			isInMultiLineComment = isInMultipleLinesComment(current, previous, isInSingleQuote, isInMultiLineComment);
		}

		private boolean isInComment() {
			return isInSingleLineComment || isInMultiLineComment;
		}

		private boolean isEndingSemicolon(char currentChar, char previousChar, boolean foundSubQueryEndingSemicolon,
										  boolean isPreviousCharInComment) {
			if (foundSubQueryEndingSemicolon) {
				return true;
			}
			return (';' == previousChar && currentChar != ';' && !isPreviousCharInComment);
		}

		private boolean isEndOfSubQuery(char currentChar) {
			return currentChar != '-' && currentChar != '/' && currentChar != ' ' && currentChar != '\n';
		}

		private boolean isCommentStart(char currentChar) {
			return currentChar == '-' || currentChar == '/';
		}

		private static boolean isInMultipleLinesComment(char currentChar, char previousChar,
														boolean isCurrentSubstringBetweenQuotes, boolean isInMultipleLinesComment) {
			if (!isCurrentSubstringBetweenQuotes && (previousChar == '/' && currentChar == '*')) {
				return true;
			} else if ((previousChar == '*' && currentChar == '/')) {
				return false;
			}
			return isInMultipleLinesComment;
		}
		private boolean isInSingleLineComment(char currentChar, char previousChar, boolean isCurrentSubstringBetweenQuotes,
											  boolean isInSingleLineComment) {
			if (!isCurrentSubstringBetweenQuotes && (previousChar == '-' && currentChar == '-')) {
				return true;
			} else if (currentChar == '\n') {
				return false;
			}
			return isInSingleLineComment;
		}
	}

}
