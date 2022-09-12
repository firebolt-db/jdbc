package com.firebolt.jdbc.statement;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.firebolt.jdbc.statement.rawstatement.RawSqlStatement;
import com.firebolt.jdbc.statement.rawstatement.RawStatementWrapper;
import com.firebolt.jdbc.statement.rawstatement.SetParamStatement;
import com.firebolt.jdbc.statement.rawstatement.SqlParamMarker;

import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class StatementUtil {

	private static final String SINGLE_LINE_COMMENTS_REGEX = "--[^\n]*\n";
	private static final String MULTI_LINE_COMMENTS_REGEX = "/\\*[^/\\*]*\\*/";

	private static final String SET_PREFIX = "set";
	private static final Pattern SET_WITH_SPACE_REGEX = Pattern.compile(SET_PREFIX + " ", Pattern.CASE_INSENSITIVE);
	private static final String[] SELECT_KEYWORDS = new String[] { "show", "select", "describe", "exists", "explain",
			"with", "call" };

	public static StatementInfoWrapper extractStatementInfo(RawSqlStatement query) {
		return StatementInfoWrapper.of(query, UUID.randomUUID().toString());
	}

	public static boolean isQuery(String sql) {
		return isQuery(sql, false);
	}

	public static boolean isQuery(String sql, boolean isCleanStatement) {
		if (StringUtils.isNotEmpty(sql)) {
			String cleanStatement = isCleanStatement ? sql : cleanStatement(sql);
			cleanStatement = cleanStatement.replace("(", "");
			return StringUtils.startsWithAny(cleanStatement.toLowerCase(), SELECT_KEYWORDS);
		} else {
			return false;
		}
	}

	public Optional<Pair<String, String>> extractPropertyFromQuery(@NonNull String cleanStatement, String sql) {
		if (StringUtils.startsWithIgnoreCase(cleanStatement, SET_PREFIX)) {
			return extractPropertyPair(cleanStatement, sql);
		}
		return Optional.empty();
	}

	public String cleanStatement(String sql) {
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
			isInSingleLineComment = isInSingleLineComment(currentChar, previousChar, isCurrentSubstringBetweenQuotes,
					isInSingleLineComment);
			isInMultipleLinesComment = isInMultipleLinesComment(currentChar, previousChar,
					isCurrentSubstringBetweenQuotes, isInMultipleLinesComment);
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
					String cleanSubstring = cleanQueryPart(sql, substringStart, subStringEnd);
					result.append(cleanSubstring);
				}
				substringStart = currentIndex + 1;
				isCurrentSubstringBetweenQuotes = !isCurrentSubstringBetweenQuotes;
				latestCommentPos = null;
			}
		}
		return result.toString().trim();
	}

	public List<StatementInfoWrapper> parseToStatementInfoWrappers(String sql) {
		return parseToSqlQueryWrapper(sql).getSubStatements().stream().map(StatementUtil::extractStatementInfo)
				.collect(Collectors.toList());
	}

	public RawStatementWrapper parseToSqlQueryWrapper(String sql) {
		List<RawSqlStatement> subQueries = new ArrayList<>();
		List<SqlParamMarker> subQueryParamMarkersPositions = new ArrayList<>();
		int subQueryStart = 0;
		int currentIndex = 0;
		char currentChar = sql.charAt(currentIndex);
		StringBuilder cleanedSubQuery = isCommentStart(currentChar) ? new StringBuilder()
				: new StringBuilder(String.valueOf(currentChar));
		boolean isCurrentSubstringBetweenQuotes = currentChar == '\'';
		boolean isInSingleLineComment = false;
		boolean isInMultipleLinesComment = false;
		boolean isInComment;
		boolean foundSubqueryEndingSemicolon = false;
		char previousChar;
		int subQueryParamsCount = 0;
		while (currentIndex++ < sql.length() - 1) {
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
				if (foundSubqueryEndingSemicolon || isEndingSemicolon(currentChar, previousChar)) {
					foundSubqueryEndingSemicolon = true;
					if (isEndOfSubquery(currentChar)) {
						subQueries.add(RawSqlStatement.of(sql.substring(subQueryStart, currentIndex),
								subQueryParamMarkersPositions, cleanedSubQuery.toString().trim()));
						subQueryParamMarkersPositions = new ArrayList<>();
						subQueryStart = currentIndex;
						foundSubqueryEndingSemicolon = false;
						cleanedSubQuery = new StringBuilder();
					}
				} else if (currentChar == '?' && !isCurrentSubstringBetweenQuotes) {
					subQueryParamMarkersPositions
							.add(new SqlParamMarker(++subQueryParamsCount, currentIndex - subQueryStart));
				} else if (currentChar == '\'') {
					isCurrentSubstringBetweenQuotes = !isCurrentSubstringBetweenQuotes;
				}
				if (!(isCommentStart(currentChar) && !isCurrentSubstringBetweenQuotes)) {
					cleanedSubQuery.append(currentChar);
				}
			}
		}
		subQueries.add(RawSqlStatement.of(sql.substring(subQueryStart, currentIndex), subQueryParamMarkersPositions,
				cleanedSubQuery.toString().trim()));
		return new RawStatementWrapper(subQueries);
	}

	private boolean isEndingSemicolon(char currentChar, char previousChar) {
		return (';' == previousChar && currentChar != ';');
	}

	private boolean isEndOfSubquery(char currentChar) {
		return currentChar != '-' && currentChar != '/' && currentChar != ' ' && currentChar != '\n';
	}

	private boolean isCommentStart(char currentChar) {
		return currentChar == '-' || currentChar == '/';
	}

	public Map<Integer, Integer> getQueryParamsPositions(String sql) {
		RawStatementWrapper rawStatementWrapper = parseToSqlQueryWrapper(sql);
		return rawStatementWrapper.getSubStatements().stream().map(RawSqlStatement::getParamMarkers)
				.flatMap(Collection::stream)
				.collect(Collectors.toMap(SqlParamMarker::getId, SqlParamMarker::getPosition));
	}

	private boolean reachedEnd(String sql, int currentIndex) {
		return currentIndex == sql.length() - 1;
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

	public static Pair<Optional<String>, Optional<String>> extractDbNameAndTableNamePairFromQuery(String sql) {
		Optional<String> from = Optional.empty();
		if (isQuery(sql)) {
			log.debug("Extracting DB and Table name for SELECT: {}", sql);
			String cleanQuery = cleanStatement(sql);
			String withoutQuotes = StringUtils.replace(cleanQuery, "'", "").trim();
			if (StringUtils.startsWithIgnoreCase(withoutQuotes, "select")) {
				int fromIndex = StringUtils.indexOfIgnoreCase(withoutQuotes, "from");
				if (fromIndex != -1) {
					from = Optional.of(withoutQuotes.substring(fromIndex + "from".length()).trim().split(" ")[0]);
				}
			} else if (StringUtils.startsWithIgnoreCase(withoutQuotes, "DESCRIBE")) {
				from = Optional.of("tables");
			} else if (StringUtils.startsWithIgnoreCase(withoutQuotes, "SHOW")) {
				from = Optional.empty(); // Depends on the information requested
			} else {
				log.debug("Could not find table name for query {}. This may happen when there is no table.", sql);
			}
		}
		return new ImmutablePair<>(extractDbNameFromFromPartOfTheQuery(from.orElse(null)),
				extractTableNameFromFromPartOfTheQuery(from.orElse(null)));
	}

	public static List<StatementInfoWrapper> replaceParameterMarksWithValues(@NonNull Map<Integer, String> params,
			@NonNull String sql) {
		RawStatementWrapper rawStatementWrapper = parseToSqlQueryWrapper(sql);
		return replaceParameterMarksWithValues(params, rawStatementWrapper);
	}

	public static List<StatementInfoWrapper> replaceParameterMarksWithValues(@NonNull Map<Integer, String> params,
			@NonNull RawStatementWrapper query) {
		List<StatementInfoWrapper> subQueries = new ArrayList<>();
		for (int subqueryIndex = 0; subqueryIndex < query.getSubStatements().size(); subqueryIndex++) {
			int currentPos;
			/*
			 * As the parameter markers are being placed then the statement sql keeps
			 * getting bigger, which is why we need to keep track of the offset
			 */
			int offset = 0;
			RawSqlStatement subQuery = query.getSubStatements().get(subqueryIndex);
			String subQueryWithParams = subQuery.getSql();

			if (params.size() != query.getTotalParams()) {
				throw new IllegalArgumentException(String.format(
						"The number of parameters passed does not equal the number of parameter markers in the SQL query. Provided: %d, Parameter markers in the SQL query: %d",
						params.size(), query.getTotalParams()));
			}
			for (SqlParamMarker param : subQuery.getParamMarkers()) {
				String value = params.get(param.getId());
				if (value == null) {
					throw new IllegalArgumentException(
							"No value for parameter marker at position: " + param.getPosition());
				}
				currentPos = param.getPosition() + offset;
				if (currentPos >= subQuery.getSql().length() + offset) {
					throw new IllegalArgumentException("The position of the parameter marker provided is invalid");
				}
				subQueryWithParams = subQueryWithParams.substring(0, currentPos) + value
						+ subQueryWithParams.substring(currentPos + 1);
				offset += value.length() - 1;
			}
			Pair<String, String> additionalParams = subQuery.getStatementType() == StatementType.PARAM_SETTING
					? ((SetParamStatement) subQuery).getAdditionalProperty()
					: null;
			subQueries.add(new StatementInfoWrapper(subQueryWithParams, UUID.randomUUID().toString(),
					subQuery.getStatementType(), additionalParams, subQuery));

		}
		return subQueries;
	}

	private static Optional<String> extractTableNameFromFromPartOfTheQuery(String from) {
		return Optional.ofNullable(from).map(s -> s.replace("\"", "")).map(fromPartOfTheQuery -> {
			if (StringUtils.contains(fromPartOfTheQuery, ".")) {
				int indexOfTableName = StringUtils.lastIndexOf(fromPartOfTheQuery, ".");
				return fromPartOfTheQuery.substring(indexOfTableName + 1);
			} else {
				return fromPartOfTheQuery;
			}
		});
	}

	private static Optional<String> extractDbNameFromFromPartOfTheQuery(String from) {
		return Optional.ofNullable(from).map(s -> s.replace("\"", ""))
				.filter(s -> StringUtils.countMatches(s, ".") == 2).map(fromPartOfTheQuery -> {
					int dbNameEndPos = StringUtils.indexOf(fromPartOfTheQuery, ".");
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

	private String cleanQueryPart(String sql, int substringStart, int substringEnd) {
		String subString = StringUtils.substring(sql, substringStart, substringEnd);
		return removeCommentsFromSubstring(subString);
	}

	private String removeCommentsFromSubstring(String subString) {
		subString = RegExUtils.replaceAll(subString, MULTI_LINE_COMMENTS_REGEX, "");
		return RegExUtils.replaceAll(subString, SINGLE_LINE_COMMENTS_REGEX,
				"\n"); /*
						 * Escape to next line to avoid words being merged when comments are added at
						 * the end of the line
						 */
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
