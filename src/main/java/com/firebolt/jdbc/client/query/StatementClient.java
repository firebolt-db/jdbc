package com.firebolt.jdbc.client.query;

import java.io.InputStream;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.StatementInfoWrapper;

public interface StatementClient {

	/**
	 * Post SQL statement
	 */
	InputStream executeSqlStatement(StatementInfoWrapper statementInfoWrapper, FireboltProperties connectionProperties,
									boolean systemEngine, int queryTimeout, boolean standardSql) throws FireboltException;

	/**
	 * Call endpoint to abort a running SQL statement
	 */
	void abortStatement(String id, FireboltProperties fireboltProperties) throws FireboltException;

	/**
	 * Abort running HTTP request of a statement
	 */
	void abortRunningHttpRequest(String id) throws FireboltException;

	boolean isStatementRunning(String statementId);
}
