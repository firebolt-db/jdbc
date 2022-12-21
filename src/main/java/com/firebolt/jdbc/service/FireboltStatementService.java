package com.firebolt.jdbc.service;

import static com.firebolt.jdbc.exception.ExceptionType.INVALID_REQUEST;

import java.io.InputStream;

import com.firebolt.jdbc.client.query.StatementClient;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.StatementInfoWrapper;

import lombok.CustomLog;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@CustomLog
public class FireboltStatementService {

	private final StatementClient statementClient;
	private final boolean systemEngine;

	/**
	 * Executes statement
	 *
	 * @param statementInfoWrapper the statement
	 * @param connectionProperties the connection properties
	 * @param queryTimeout         query timeout
	 * @param maxRows              max rows
	 * @param standardSql          indicates if standard sql should be used
	 * @return an InputStream with the result
	 */
	public InputStream execute(@NonNull StatementInfoWrapper statementInfoWrapper,
			@NonNull FireboltProperties connectionProperties, int queryTimeout, int maxRows, boolean standardSql)
			throws FireboltException {
		return statementClient.postSqlStatement(statementInfoWrapper, connectionProperties, systemEngine, queryTimeout,
				maxRows, standardSql);
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
}
