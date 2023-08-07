package com.firebolt.jdbc.service;

import static com.firebolt.jdbc.exception.ExceptionType.INVALID_REQUEST;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import com.firebolt.jdbc.client.query.StatementClient;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.statement.StatementInfoWrapper;

import com.firebolt.jdbc.statement.StatementType;
import com.firebolt.jdbc.statement.rawstatement.QueryRawStatement;
import com.firebolt.jdbc.util.CloseableUtil;
import com.firebolt.jdbc.util.InputStreamUtil;
import lombok.CustomLog;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@CustomLog
public class FireboltStatementService {

	private static final String UNKNOWN_TABLE_NAME = "unknown";
	private final StatementClient statementClient;
	private final boolean systemEngine;

	/**
	 * Executes statement
	 *
	 * @param statementInfoWrapper the statement info
	 * @param properties the connection properties
	 * @param queryTimeout         query timeout
	 * @param maxRows              max rows
	 * @param standardSql          indicates if standard sql should be used
	 * @param statement           the statement
	 * @return an InputStream with the result
	 */
	public Optional<ResultSet> execute(StatementInfoWrapper statementInfoWrapper,
									   FireboltProperties properties, int queryTimeout, int maxRows, boolean standardSql,
									   FireboltStatement statement)
			throws SQLException {
		InputStream is = statementClient.executeSqlStatement(statementInfoWrapper, properties, systemEngine, queryTimeout,standardSql);
		if (statementInfoWrapper.getType() == StatementType.QUERY) {
			return Optional.of(createResultSet(is, (QueryRawStatement) statementInfoWrapper.getInitialStatement(), properties, statement, maxRows));
		} else {
			// If the statement is not a query, read all bytes from the input stream and close it.
			// This is needed otherwise the stream with the server will be closed after having received the first chunk of data (resulting in incomplete inserts).
			InputStreamUtil.readAllBytes(is);
			CloseableUtil.close(is);
		}
		return Optional.empty();
	}

	public void abortStatement(@NonNull String statementId, @NonNull FireboltProperties properties)
			throws FireboltException {
		if (systemEngine) {
			throw new FireboltException("Cannot cancel a statement using a system engine", INVALID_REQUEST);
		} else {
			statementClient.abortStatement(statementId, properties);
		}
	}

	public void abortStatementHttpRequest(@NonNull String statementId) throws FireboltException {
		statementClient.abortRunningHttpRequest(statementId);
	}

	public boolean isStatementRunning(String statementId) {
		return statementClient.isStatementRunning(statementId);
	}

	private FireboltResultSet createResultSet(InputStream inputStream, QueryRawStatement initialQuery, FireboltProperties properties, FireboltStatement statement, int maxRows)
			throws SQLException {
		return new FireboltResultSet(inputStream, Optional.ofNullable(initialQuery.getTable()).orElse(UNKNOWN_TABLE_NAME),
				Optional.ofNullable(initialQuery.getDatabase()).orElse(properties.getDatabase()),
				properties.getBufferSize(), maxRows, properties.isCompress(), statement,
				properties.isLogResultSet());
	}
}
