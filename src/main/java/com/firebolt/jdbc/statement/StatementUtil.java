package com.firebolt.jdbc.statement;

import com.firebolt.jdbc.statement.rawstatement.RawStatement;
import com.firebolt.jdbc.statement.rawstatement.RawStatementWrapper;
import com.firebolt.jdbc.statement.rawstatement.SetParamRawStatement;
import com.firebolt.jdbc.util.StringUtil;
import lombok.CustomLog;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@UtilityClass
@CustomLog
public class StatementUtil {

	private static final String SET_PREFIX = "set";
	private static final Pattern SET_WITH_SPACE_REGEX = Pattern.compile(SET_PREFIX + " ", Pattern.CASE_INSENSITIVE);
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
		return parseToRawStatementWrapper(sql).getSubStatements().stream().map(StatementInfoWrapper::of)
				.collect(Collectors.toList());
	}

	/**
	 * Parse sql statement to a {@link RawStatementWrapper}. The method construct
	 * the {@link RawStatementWrapper} by splitting it in a list of sub-statements
	 * (supports multistatements)
	 * 
	 * @param sql the sql statement
	 * @return a list of {@link StatementInfoWrapper}
	 */
	public RawStatementWrapper parseToRawStatementWrapper(String sql) {
		if (sql.isEmpty()) {
			return new RawStatementWrapper(List.of());
		}
		List<RawStatement> subStatements = new ArrayList<>();
		List<ParamMarker> subStatementParamMarkersPositions = new ArrayList<>();
		int subQueryStart = 0;
		int currentIndex = 0;
		char currentChar = sql.charAt(currentIndex);
		StringBuilder cleanedSubQuery = isCommentStart(currentChar) ? new StringBuilder()
				: new StringBuilder(String.valueOf(currentChar));
		boolean isCurrentSubstringBetweenQuotes = currentChar == '\'';
		boolean isCurrentSubstringBetweenDoubleQuotes = currentChar == '"';
		boolean isInSingleLineComment = false;
		boolean isInMultipleLinesComment = false;
		boolean isInComment = false;
		boolean foundSubqueryEndingSemicolon = false;
		char previousChar;
		int subQueryParamsCount = 0;
		boolean isPreviousCharInComment;
		while (currentIndex++ < sql.length() - 1) {
			isPreviousCharInComment = isInComment;
			previousChar = currentChar;
			currentChar = sql.charAt(currentIndex);
			isInSingleLineComment = isInSingleLineComment(currentChar, previousChar, isCurrentSubstringBetweenQuotes,
					isInSingleLineComment);
			isInMultipleLinesComment = isInMultipleLinesComment(currentChar, previousChar,
					isCurrentSubstringBetweenQuotes, isInMultipleLinesComment);
			isInComment = isInSingleLineComment || isInMultipleLinesComment;
			if (!isInComment) {
				// Although the ending semicolon may have been found, we need to include any
				// potential comments to the subquery
				if (!isCurrentSubstringBetweenQuotes && isEndingSemicolon(currentChar, previousChar,
						foundSubqueryEndingSemicolon, isPreviousCharInComment)) {
					foundSubqueryEndingSemicolon = true;
					if (isEndOfSubquery(currentChar)) {
						subStatements.add(RawStatement.of(sql.substring(subQueryStart, currentIndex),
								subStatementParamMarkersPositions, cleanedSubQuery.toString().trim()));
						subStatementParamMarkersPositions = new ArrayList<>();
						subQueryStart = currentIndex;
						foundSubqueryEndingSemicolon = false;
						cleanedSubQuery = new StringBuilder();
					}
				} else if (currentChar == '?' && !isCurrentSubstringBetweenQuotes
						&& !isCurrentSubstringBetweenDoubleQuotes) {
					subStatementParamMarkersPositions
							.add(new ParamMarker(++subQueryParamsCount, currentIndex - subQueryStart));
				} else if (currentChar == '\'') {
					isCurrentSubstringBetweenQuotes = !isCurrentSubstringBetweenQuotes;
				} else if (currentChar == '"') {
					isCurrentSubstringBetweenDoubleQuotes = !isCurrentSubstringBetweenDoubleQuotes;
				}
				if (!(isCommentStart(currentChar) && !isCurrentSubstringBetweenQuotes)) {
					cleanedSubQuery.append(currentChar);
				}
			}
		}
		subStatements.add(RawStatement.of(sql.substring(subQueryStart, currentIndex), subStatementParamMarkersPositions,
				cleanedSubQuery.toString().trim()));
		return new RawStatementWrapper(subStatements);
	}

	private boolean isEndingSemicolon(char currentChar, char previousChar, boolean foundSubqueryEndingSemicolon,
			boolean isPreviousCharInComment) {
		if (foundSubqueryEndingSemicolon) {
			return true;
		}
		return (';' == previousChar && currentChar != ';' && !isPreviousCharInComment);
	}

	private boolean isEndOfSubquery(char currentChar) {
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
	public Map<Integer, Integer> getParamMarketsPositions(String sql) {
		RawStatementWrapper rawStatementWrapper = parseToRawStatementWrapper(sql);
		return rawStatementWrapper.getSubStatements().stream().map(RawStatement::getParamMarkers)
				.flatMap(Collection::stream).collect(Collectors.toMap(ParamMarker::getId, ParamMarker::getPosition));
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
	public static List<StatementInfoWrapper> replaceParameterMarksWithValues(@NonNull Map<Integer, String> params,
			@NonNull String sql) {
		RawStatementWrapper rawStatementWrapper = parseToRawStatementWrapper(sql);
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
	public List<StatementInfoWrapper> replaceParameterMarksWithValues(@NonNull Map<Integer, String> params,
			@NonNull RawStatementWrapper rawStatement) {
		List<StatementInfoWrapper> subQueries = new ArrayList<>();
		for (int subqueryIndex = 0; subqueryIndex < rawStatement.getSubStatements().size(); subqueryIndex++) {
			int currentPos;
			/*
			 * As the parameter markers are being placed then the statement sql keeps
			 * getting bigger, which is why we need to keep track of the offset
			 */
			int offset = 0;
			RawStatement subQuery = rawStatement.getSubStatements().get(subqueryIndex);
			String subQueryWithParams = subQuery.getSql();

			if (params.size() != rawStatement.getTotalParams()) {
				throw new IllegalArgumentException(String.format(
						"The number of parameters passed does not equal the number of parameter markers in the SQL query. Provided: %d, Parameter markers in the SQL query: %d",
						params.size(), rawStatement.getTotalParams()));
			}
			for (ParamMarker param : subQuery.getParamMarkers()) {
				String value = params.get(param.getId());
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
			subQueries.add(new StatementInfoWrapper(subQueryWithParams, subQuery.getStatementType(), additionalParams, subQuery));
		}
		return subQueries;
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
}
