package com.firebolt.jdbc.service;

import com.firebolt.jdbc.client.query.StatementClient;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.resultset.compress.LZ4InputStream;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementType;
import com.firebolt.jdbc.statement.rawstatement.QueryRawStatement;
import com.firebolt.jdbc.util.CloseableUtil;
import com.firebolt.jdbc.util.InputStreamUtil;
import lombok.CustomLog;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
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
	 * @param statement           the statement
	 * @return an InputStream with the result
	 */
	public Optional<ResultSet> execute(StatementInfoWrapper statementInfoWrapper,
									   FireboltProperties properties, FireboltStatement statement)
			throws SQLException {
		int queryTimeout = statement.getQueryTimeout();
		InputStream is = statementClient.executeSqlStatement(statementInfoWrapper, properties, queryTimeout, false);
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

	public String executeAsyncStatement(StatementInfoWrapper statementInfoWrapper,
									   FireboltProperties properties, FireboltStatement statement)
			throws SQLException {
		int queryTimeout = statement.getQueryTimeout();
		boolean systemEngine = properties.isSystemEngine();
		if (systemEngine) {
			throw new FireboltException("Cannot execute async statement on system engine");
		}
        try (InputStream is = statementClient.executeSqlStatement(statementInfoWrapper, properties, queryTimeout, true)) {
			InputStreamReader inputStreamReader;
			if (properties.isCompress()) {
				inputStreamReader = new InputStreamReader(new LZ4InputStream(is), UTF_8);
			} else {
				inputStreamReader = new InputStreamReader(is, UTF_8);
			}
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader, properties.getBufferSize());
			return new JSONObject(bufferedReader.lines().reduce(String::concat).orElseThrow(IOException::new)).optString("token");
        } catch (IOException e) {
			throw new FireboltException("Cannot read response from DB: error while reading asyncToken ", e);
        } catch (JSONException e) {
			throw new FireboltException("Cannot read token from response: error while reading asyncToken ", e);
		} catch (Exception e) {
			throw new FireboltException("Error while reading query response", e);
		}
    }

	/**
	 * Executes statement with files
	 *
	 * @param statementInfoWrapper the statement info
	 * @param properties the connection properties
	 * @param statement the statement
	 * @param files map of file identifiers to file contents
	 * @return an Optional ResultSet if the statement returns results
	 */
	public Optional<ResultSet> executeWithFiles(StatementInfoWrapper statementInfoWrapper,
												 FireboltProperties properties, FireboltStatement statement,
												 Map<String, byte[]> files)
			throws SQLException {
		int queryTimeout = statement.getQueryTimeout();
		InputStream is = statementClient.executeSqlStatementWithFiles(statementInfoWrapper, properties, queryTimeout, false, files);
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

	public void abortStatement(@NonNull String statementLabel, @NonNull FireboltProperties properties) throws SQLException {
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
