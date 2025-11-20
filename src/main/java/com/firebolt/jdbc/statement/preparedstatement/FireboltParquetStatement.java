package com.firebolt.jdbc.statement.preparedstatement;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementUtil;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.createValidator;

@CustomLog
public class FireboltParquetStatement extends FireboltStatement {

	private List<StatementInfoWrapper> parsedStatements;
	private final FireboltStatementService statementService;
	private final FireboltConnection connection;

	public FireboltParquetStatement(FireboltStatementService statementService, FireboltConnection connection) {
        super(statementService, connection.getSessionProperties(), connection);
        this.statementService = statementService;
        this.connection = connection;
	}

    @Override
	public ResultSet executeQuery(String sql) throws SQLException {
		throw new FireboltException("Cannot call executeQuery(String sql) on a FireboltParquetStatement. Use executeQuery(String, Map<String, byte[]>) or similar overloads instead.");
	}

	public ResultSet executeQuery(String sql, Map<String, byte[]> files) throws SQLException {
		validateStatementIsNotClosed();
		setSql(sql);
		validateFiles(files);
		return executeQuery(parsedStatements, files);
	}

	public ResultSet executeQueryWithInputStreams(String sql, Map<String, InputStream> inputStreams) throws SQLException {
		validateStatementIsNotClosed();
		setSql(sql);
		validateInputStreams(inputStreams);
		Map<String, byte[]> files = convertInputStreamsToBytes(inputStreams);
		return executeQuery(parsedStatements, files);
	}

	public ResultSet executeQueryWithFiles(String sql, Map<String, File> fileMap) throws SQLException {
		validateStatementIsNotClosed();
		setSql(sql);
		validateFileMap(fileMap);
		Map<String, byte[]> files = convertFilesToBytes(fileMap);
		return executeQuery(parsedStatements, files);
	}

	@Override
	protected ResultSet executeQuery(List<StatementInfoWrapper> statementInfoList) throws SQLException {
		throw new FireboltException("Cannot call executeQuery(List<StatementInfoWrapper>) directly. Use executeQuery(String, Map<String, byte[]>) or similar overloads instead.");
	}

	/**
	 * Internal method to execute query with files
	 */
	private ResultSet executeQuery(List<StatementInfoWrapper> statementInfoList, Map<String, byte[]> files) throws SQLException {
		StatementInfoWrapper query = getOneQueryStatementInfo(statementInfoList);
		Optional<ResultSet> resultSet = executeWithFiles(Collections.singletonList(query), files);
		synchronized (this) {
			return resultSet.orElseThrow(() -> new FireboltException("Could not return ResultSet - the query returned no result."));
		}
	}

	@Override
	public boolean execute(String sql) throws SQLException {
		throw new FireboltException("Cannot call execute(String sql) on a FireboltParquetStatement. Use execute(String, Map<String, byte[]>) or similar overloads instead.");
	}

	public boolean execute(String sql, Map<String, byte[]> files) throws SQLException {
		validateStatementIsNotClosed();
		setSql(sql);
		validateFiles(files);
		return execute(parsedStatements, files).isPresent();
	}

	public boolean executeWithInputStreams(String sql, Map<String, InputStream> inputStreams) throws SQLException {
		validateStatementIsNotClosed();
		setSql(sql);
		validateInputStreams(inputStreams);
		Map<String, byte[]> files = convertInputStreamsToBytes(inputStreams);
		return execute(parsedStatements, files).isPresent();
	}

	public boolean executeWithFiles(String sql, Map<String, File> fileMap) throws SQLException {
		validateStatementIsNotClosed();
		setSql(sql);
		validateFileMap(fileMap);
		Map<String, byte[]> files = convertFilesToBytes(fileMap);
		return execute(parsedStatements, files).isPresent();
	}

	@Override
	protected Optional<ResultSet> execute(List<StatementInfoWrapper> statements) throws SQLException {
		throw new FireboltException("Cannot call execute(List<StatementInfoWrapper>) directly. Use execute(String, Map<String, byte[]>) or similar overloads instead.");
	}

	/**
	 * Internal method to execute with files
	 */
	private Optional<ResultSet> execute(List<StatementInfoWrapper> statements, Map<String, byte[]> files) throws SQLException {
		return executeWithFiles(statements, files);
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		throw new FireboltException("Cannot call executeUpdate(String sql) on a FireboltParquetStatement. Use executeUpdate(String, Map<String, byte[]>) or similar overloads instead.");
	}

	public int executeUpdate(String sql, Map<String, byte[]> files) throws SQLException {
		validateStatementIsNotClosed();
		setSql(sql);
		validateFiles(files);
		return executeUpdate(parsedStatements, files);
	}

	public int executeUpdateWithInputStreams(String sql, Map<String, InputStream> inputStreams) throws SQLException {
		validateStatementIsNotClosed();
		setSql(sql);
		validateInputStreams(inputStreams);
		Map<String, byte[]> files = convertInputStreamsToBytes(inputStreams);
		return executeUpdate(parsedStatements, files);
	}

	public int executeUpdateWithFiles(String sql, Map<String, File> fileMap) throws SQLException {
		validateStatementIsNotClosed();
		setSql(sql);
		validateFileMap(fileMap);
		Map<String, byte[]> files = convertFilesToBytes(fileMap);
		return executeUpdate(parsedStatements, files);
	}

	@Override
	protected int executeUpdate(List<StatementInfoWrapper> sql) throws SQLException {
		throw new FireboltException("Cannot call executeUpdate(List<StatementInfoWrapper>) directly. Use executeUpdate(String, Map<String, byte[]>) or similar overloads instead.");
	}

	/**
	 * Internal method to execute update with files
	 */
	private int executeUpdate(List<StatementInfoWrapper> statements, Map<String, byte[]> files) throws SQLException {
		executeWithFiles(statements, files);
		return validateAndCloseUpdateResults();
	}

	/**
	 * Executes statements with files
	 */
	private Optional<ResultSet> executeWithFiles(List<StatementInfoWrapper> statements, Map<String, byte[]> files) throws SQLException {
		return executeStatements(statements, statement -> executeWithFiles(statement, files));
	}

	/**
	 * Executes a single statement with files
	 */
	private Optional<ResultSet> executeWithFiles(StatementInfoWrapper statementInfoWrapper, Map<String, byte[]> files) throws SQLException {
		return executeStatement(statementInfoWrapper,
				() -> statementService.executeWithFiles(statementInfoWrapper, sessionProperties, this, files),
				"statement with files");
	}

	private void validateSqlParameter(String sql) throws SQLException {
		if (StringUtils.isBlank(sql)) {
			throw new FireboltException("SQL cannot be null or blank");
		}
	}

	/**
	 * Sets the SQL statement for this parquet statement
	 *
	 * @param sql the SQL statement to execute
	 * @throws SQLException if the SQL is invalid or the statement is closed
	 */
	private void setSql(String sql) throws SQLException {
		validateStatementIsNotClosed();
		validateSqlParameter(sql);
		// Parse and validate the SQL statement
		this.parsedStatements = StatementUtil.parseToStatementInfoWrappers(sql);
		parsedStatements.forEach(statement -> createValidator(statement.getInitialStatement(), connection).validate(statement.getInitialStatement()));
	}


	/**
	 * Validates that the files map is not null or empty
	 */
	private void validateFiles(Map<String, byte[]> files) throws SQLException {
		if (files == null || files.isEmpty()) {
			throw new FireboltException("Files map cannot be null or empty");
		}
		for (Map.Entry<String, byte[]> entry : files.entrySet()) {
			if (entry.getKey() == null) {
				throw new FireboltException("File identifier cannot be null");
			}
			if (entry.getValue() == null) {
				throw new FireboltException("File content for identifier '" + entry.getKey() + "' cannot be null");
			}
		}
	}

	/**
	 * Validates that the input streams map is not null or empty and contains valid entries
	 *
	 * @param inputStreams map of file identifiers to input streams
	 * @throws SQLException if the map is null, empty, or contains invalid entries
	 */
	private void validateInputStreams(Map<String, InputStream> inputStreams) throws SQLException {
		if (inputStreams == null || inputStreams.isEmpty()) {
			throw new FireboltException("Input streams map cannot be null or empty");
		}
		for (Map.Entry<String, InputStream> entry : inputStreams.entrySet()) {
			String identifier = entry.getKey();
			InputStream inputStream = entry.getValue();
			if (identifier == null) {
				throw new FireboltException("File identifier cannot be null");
			}
			if (inputStream == null) {
				throw new FireboltException("Input stream for identifier '" + identifier + "' cannot be null");
			}
		}
	}

	/**
	 * Converts a map of InputStreams to a map of byte arrays
	 */
	private Map<String, byte[]> convertInputStreamsToBytes(Map<String, InputStream> inputStreams) throws SQLException {
		Map<String, byte[]> files = new HashMap<>();
		for (Map.Entry<String, InputStream> entry : inputStreams.entrySet()) {
			String identifier = entry.getKey();
			InputStream inputStream = entry.getValue();
			try {
				files.put(identifier, inputStream.readAllBytes());
			} catch (IOException e) {
				throw new SQLException("Failed to read input stream for identifier '" + identifier + "'", e);
			}
		}
		return files;
	}

	/**
	 * Validates that the file map is not null or empty and contains valid entries
	 *
	 * @param fileMap map of file identifiers to file objects
	 * @throws SQLException if the map is null, empty, or contains invalid entries
	 */
	private void validateFileMap(Map<String, File> fileMap) throws SQLException {
		if (fileMap == null || fileMap.isEmpty()) {
			throw new FireboltException("File map cannot be null or empty");
		}
		for (Map.Entry<String, File> entry : fileMap.entrySet()) {
			String identifier = entry.getKey();
			File file = entry.getValue();
			if (identifier == null) {
				throw new FireboltException("File identifier cannot be null");
			}
			if (file == null) {
				throw new FireboltException("File for identifier '" + identifier + "' cannot be null");
			}
			if (!file.exists()) {
				throw new FireboltException("File does not exist: " + file.getAbsolutePath());
			}
			if (!file.isFile()) {
				throw new FireboltException("Path is not a file: " + file.getAbsolutePath());
			}
		}
	}

	/**
	 * Converts a map of File objects to a map of byte arrays
	 */
	private Map<String, byte[]> convertFilesToBytes(Map<String, File> fileMap) throws SQLException {
		Map<String, byte[]> files = new HashMap<>();
		for (Map.Entry<String, File> entry : fileMap.entrySet()) {
			String identifier = entry.getKey();
			File file = entry.getValue();
			try (FileInputStream fis = new FileInputStream(file)) {
				files.put(identifier, fis.readAllBytes());
			} catch (IOException e) {
				throw new SQLException("Failed to read file: " + file.getAbsolutePath(), e);
			}
		}
		return files;
	}



}
