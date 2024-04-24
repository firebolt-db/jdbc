package com.firebolt.jdbc.service;

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

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

import static java.util.Optional.ofNullable;

@RequiredArgsConstructor
@CustomLog
public class FireboltStatementService {

	private static final String UNKNOWN_TABLE_NAME = "unknown";
	private final StatementClient statementClient;

	/**
	 * Executes statement
	 *
	 * @param statementInfoWrapper the statement info
	 * @param properties the connection properties
	 * @param standardSql          indicates if standard sql should be used
	 * @param statement           the statement
	 * @return an InputStream with the result
	 */
	public Optional<ResultSet> execute(StatementInfoWrapper statementInfoWrapper,
									   FireboltProperties properties, boolean standardSql, FireboltStatement statement)
			throws SQLException {
		int queryTimeout = statement.getQueryTimeout();
		boolean systemEngine = properties.isSystemEngine();
		InputStream is = statementClient.executeSqlStatement(statementInfoWrapper, properties, systemEngine, queryTimeout, standardSql);
		if (statementInfoWrapper.getType() == StatementType.QUERY) {
			return Optional.of(createResultSet(is, (QueryRawStatement) statementInfoWrapper.getInitialStatement(), properties, statement));
		} else {
			// If the statement is not a query, read all bytes from the input stream and close it.
			// This is needed otherwise the stream with the server will be closed after having received the first chunk of data (resulting in incomplete inserts).
			InputStreamUtil.readAllBytes(is);
			CloseableUtil.close(is);
		}
		return Optional.empty();
	}

	public void abortStatement(@NonNull String statementLabel, @NonNull FireboltProperties properties) throws FireboltException {
		statementClient.abortStatement(statementLabel, properties);
	}

	public boolean isStatementRunning(String statementLabel) {
		return statementClient.isStatementRunning(statementLabel);
	}

	private FireboltResultSet createResultSet(InputStream inputStream, QueryRawStatement initialQuery, FireboltProperties properties, FireboltStatement statement)
			throws SQLException {
		return new FireboltResultSet(inputStream,
				ofNullable(initialQuery.getTable()).orElse(UNKNOWN_TABLE_NAME),
				ofNullable(initialQuery.getDatabase()).orElse(properties.getDatabase()),
				properties.getBufferSize(), properties.isCompress(),
				statement, properties.isLogResultSet());
	}
}
