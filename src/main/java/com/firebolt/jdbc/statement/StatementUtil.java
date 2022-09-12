package com.firebolt.jdbc.statement;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.RegExUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.firebolt.jdbc.statement.rawstatement.RawStatement;
import com.firebolt.jdbc.statement.rawstatement.RawStatementWrapper;
import com.firebolt.jdbc.statement.rawstatement.SetParamRawStatement;
import com.firebolt.jdbc.statement.rawstatement.SqlParamMarker;

import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
public class StatementUtil {

	private static final String SET_PREFIX = "set";
	private static final Pattern SET_WITH_SPACE_REGEX = Pattern.compile(SET_PREFIX + " ", Pattern.CASE_INSENSITIVE);
	private static final String[] SELECT_KEYWORDS = new String[] { "show", "select", "describe", "exists", "explain",
			"with", "call" };

	public static StatementInfoWrapper extractStatementInfo(RawStatement query) {
		return StatementInfoWrapper.of(query);
	}

	public static boolean isQuery(String cleanSql) {
		if (StringUtils.isNotEmpty(cleanSql)) {
			cleanSql = cleanSql.replace("(", "");
			return StringUtils.startsWithAny(cleanSql.toLowerCase(), SELECT_KEYWORDS);
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

	public List<StatementInfoWrapper> parseToStatementInfoWrappers(String sql) {
		return parseToRawStatementWrapper(sql).getSubStatements().stream().map(StatementUtil::extractStatementInfo)
				.collect(Collectors.toList());
	}

	public RawStatementWrapper parseToRawStatementWrapper(String sql) {
		List<RawStatement> subQueries = new ArrayList<>();
		List<SqlParamMarker> subStatementParamMarkersPositions = new ArrayList<>();
		int subQueryStart = 0;
		int currentIndex = 0;
		char currentChar = sql.charAt(currentIndex);
		StringBuilder cleanedSubQuery = isCommentStart(currentChar) ? new StringBuilder()
				: new StringBuilder(String.valueOf(currentChar));
		boolean isCurrentSubstringBetweenQuotes = currentChar == '\'';
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
						subQueries.add(RawStatement.of(sql.substring(subQueryStart, currentIndex),
								subStatementParamMarkersPositions, cleanedSubQuery.toString().trim()));
						subStatementParamMarkersPositions = new ArrayList<>();
						subQueryStart = currentIndex;
						foundSubqueryEndingSemicolon = false;
						cleanedSubQuery = new StringBuilder();
					}
				} else if (currentChar == '?' && !isCurrentSubstringBetweenQuotes) {
					subStatementParamMarkersPositions
							.add(new SqlParamMarker(++subQueryParamsCount, currentIndex - subQueryStart));
				} else if (currentChar == '\'') {
					isCurrentSubstringBetweenQuotes = !isCurrentSubstringBetweenQuotes;
				}
				if (!(isCommentStart(currentChar) && !isCurrentSubstringBetweenQuotes)) {
					cleanedSubQuery.append(currentChar);
				}
			}
		}
		subQueries.add(RawStatement.of(sql.substring(subQueryStart, currentIndex), subStatementParamMarkersPositions,
				cleanedSubQuery.toString().trim()));
		return new RawStatementWrapper(subQueries);
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

	public Map<Integer, Integer> getQueryParamsPositions(String sql) {
		RawStatementWrapper rawStatementWrapper = parseToRawStatementWrapper(sql);
		return rawStatementWrapper.getSubStatements().stream().map(RawStatement::getParamMarkers)
				.flatMap(Collection::stream)
				.collect(Collectors.toMap(SqlParamMarker::getId, SqlParamMarker::getPosition));
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

	public static Pair<Optional<String>, Optional<String>> extractDbNameAndTableNamePairFromQuery(String cleanSql) {
		Optional<String> from = Optional.empty();
		if (isQuery(cleanSql)) {
			log.debug("Extracting DB and Table name for SELECT: {}", cleanSql);
			String withoutQuotes = StringUtils.replace(cleanSql, "'", "").trim();
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
				log.debug("Could not find table name for query {}. This may happen when there is no table.", cleanSql);
			}
		}
		return new ImmutablePair<>(extractDbNameFromFromPartOfTheQuery(from.orElse(null)),
				extractTableNameFromFromPartOfTheQuery(from.orElse(null)));
	}

	public static List<StatementInfoWrapper> replaceParameterMarksWithValues(@NonNull Map<Integer, String> params,
			@NonNull String sql) {
		RawStatementWrapper rawStatementWrapper = parseToRawStatementWrapper(sql);
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
			RawStatement subQuery = query.getSubStatements().get(subqueryIndex);
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
					? ((SetParamRawStatement) subQuery).getAdditionalProperty()
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

	private Optional<Pair<String, String>> extractPropertyPair(String cleanStatement, String sql) {
		String setQuery = RegExUtils.removeFirst(cleanStatement, SET_WITH_SPACE_REGEX);
		String[] values = StringUtils.split(setQuery, "=");
		if (values.length == 2) {
			return Optional.of(Pair.of(values[0].trim(), StringUtils.removeEnd(values[1], ";").trim()));
		} else {
			throw new IllegalArgumentException(
					"Cannot parse the additional properties provided in the statement: " + sql);
		}
	}
}
