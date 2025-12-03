package com.firebolt.jdbc.statement.preparedstatement;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementUtil;
import lombok.CustomLog;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
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
	public boolean execute(String sql) throws SQLException {
		throw new FireboltException("Cannot call execute(String sql) on a FireboltParquetStatement. Use execute(String, Map<String, byte[]>) instead.");
	}

	public boolean execute(String sql, Map<String, byte[]> files) throws SQLException {
		validateStatementIsNotClosed();
		setSql(sql);
		validateFiles(files);
		return executeWithFiles(parsedStatements, files).isPresent();
	}

	@Override
	protected Optional<ResultSet> execute(List<StatementInfoWrapper> statements) throws SQLException {
		throw new FireboltException("Cannot call execute(List<StatementInfoWrapper>) directly. Use execute(String, Map<String, byte[]>) instead.");
	}

	@Override
	public int executeUpdate(String sql) throws SQLException {
		throw new FireboltException("Cannot call executeUpdate(String sql) on a FireboltParquetStatement. Use executeUpdate(String, Map<String, byte[]>) instead.");
	}

	public int executeUpdate(String sql, Map<String, byte[]> files) throws SQLException {
		validateStatementIsNotClosed();
		setSql(sql);
		validateFiles(files);
        executeWithFiles(parsedStatements, files);
        return validateAndCloseUpdateResults();
	}

	@Override
	protected int executeUpdate(List<StatementInfoWrapper> sql) throws SQLException {
		throw new FireboltException("Cannot call executeUpdate(List<StatementInfoWrapper>) directly. Use executeUpdate(String, Map<String, byte[]>) instead.");
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
	 * Executes a statement asynchronously with files
	 *
	 * @param sql the SQL statement to execute
	 * @param files map of file identifiers to file contents
	 * @throws SQLException if the SQL is invalid, the statement is closed, or execution fails
	 */
	public void executeAsync(String sql, Map<String, byte[]> files) throws SQLException {
		validateFiles(files);
        setSql(sql);
		StatementInfoWrapper query = parsedStatements.get(0);
		executeAsyncStatement(query,
				() -> statementService.executeAsyncStatementWithFiles(query, sessionProperties, this, files),
				"statement with files");
	}
}
