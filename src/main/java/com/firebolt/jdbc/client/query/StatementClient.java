package com.firebolt.jdbc.client.query;

import java.io.InputStream;
import java.sql.SQLException;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.StatementInfoWrapper;

public interface StatementClient {

	/**
	 * Post SQL statement
	 */
	InputStream executeSqlStatement(StatementInfoWrapper statementInfoWrapper, FireboltProperties connectionProperties,
									boolean systemEngine, int queryTimeout, boolean standardSql) throws SQLException;

	/**
	 * Call endpoint to abort a running SQL statement
	 */
	void abortStatement(String label, FireboltProperties fireboltProperties) throws SQLException;

	boolean isStatementRunning(String statementLabel);
}
