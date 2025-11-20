package com.firebolt.jdbc.statement.preparedstatement;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.rawstatement.StatementValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.createValidator;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltParquetStatementTest {

	@Mock
	private FireboltStatementService mockStatementService;
	@Mock
	private FireboltConnection mockConnection;
	@Mock
	private FireboltProperties mockSessionProperties;
	@Mock
	private StatementValidator mockStatementValidator;
	@Mock
	private FireboltResultSet mockResultSet;

	private FireboltParquetStatement parquetStatement;
	private byte[] testFileContent;

	@BeforeEach
	void setUp() {
		when(mockConnection.getSessionProperties()).thenReturn(mockSessionProperties);
		parquetStatement = new FireboltParquetStatement(mockStatementService, mockConnection);
		testFileContent = "test parquet content".getBytes();
	}

	@Test
	void shouldThrowExceptionWhenCallingExecuteWithStringOnly() {
		FireboltException exception = assertThrows(FireboltException.class,
				() -> parquetStatement.execute("SELECT 1"));
		assertTrue(exception.getMessage().contains("Cannot call execute(String sql)"));
	}

	@Test
	void shouldThrowExceptionWhenCallingExecuteUpdateWithStringOnly() {
		FireboltException exception = assertThrows(FireboltException.class,
				() -> parquetStatement.executeUpdate("INSERT INTO test VALUES (1)"));
		assertTrue(exception.getMessage().contains("Cannot call executeUpdate(String sql)"));
	}

	@Test
	void shouldExecuteWithByteArrays() throws SQLException {
		String sql = "SELECT id, name FROM read_parquet('upload://file1')";
		Map<String, byte[]> files = new HashMap<>();
		files.put("file1", testFileContent);

		when(mockStatementService.executeWithFiles(any(StatementInfoWrapper.class), eq(mockSessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.of(mockResultSet));

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(mockConnection))).thenReturn(mockStatementValidator);
			doNothing().when(mockStatementValidator).validate(any());

			boolean hasResultSet = parquetStatement.execute(sql, files);

			assertTrue(hasResultSet);
			verify(mockStatementService).executeWithFiles(any(StatementInfoWrapper.class), eq(mockSessionProperties), eq(parquetStatement), anyMap());
		}
	}


	@Test
	void shouldExecuteUpdateWithByteArrays() throws SQLException {
		String sql = "INSERT INTO test SELECT id, name FROM read_parquet('upload://file1')";
		Map<String, byte[]> files = new HashMap<>();
		files.put("file1", testFileContent);

		when(mockStatementService.executeWithFiles(any(StatementInfoWrapper.class), eq(mockSessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.empty());

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(mockConnection))).thenReturn(mockStatementValidator);
			doNothing().when(mockStatementValidator).validate(any());

			parquetStatement.executeUpdate(sql, files);

			verify(mockStatementService).executeWithFiles(any(StatementInfoWrapper.class), eq(mockSessionProperties), eq(parquetStatement), anyMap());
		}
	}

	@Test
	void shouldExecuteWithNoResultSet() throws SQLException {
		String sql = "INSERT INTO test VALUES (1)";
		Map<String, byte[]> files = new HashMap<>();
		files.put("file1", testFileContent);

		when(mockStatementService.executeWithFiles(any(StatementInfoWrapper.class), eq(mockSessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.empty());

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(mockConnection))).thenReturn(mockStatementValidator);
			doNothing().when(mockStatementValidator).validate(any());

			boolean hasResultSet = parquetStatement.execute(sql, files);

			assertFalse(hasResultSet);
		}
	}

	@Test
	void shouldThrowExceptionWhenCallingExecuteWithList() {
		FireboltException exception = assertThrows(FireboltException.class,
				() -> parquetStatement.execute(java.util.Collections.emptyList()));
		assertTrue(exception.getMessage().contains("Cannot call execute(List<StatementInfoWrapper>)"));
	}

	@Test
	void shouldThrowExceptionWhenCallingExecuteUpdateWithList() {
		FireboltException exception = assertThrows(FireboltException.class,
				() -> parquetStatement.executeUpdate(java.util.Collections.emptyList()));
		assertTrue(exception.getMessage().contains("Cannot call executeUpdate(List<StatementInfoWrapper>)"));
	}

	// Validation tests for validateFiles
	@ParameterizedTest
	@CsvSource({"null", "empty"})
	void shouldThrowExceptionWhenFilesMapIsInvalid(String type) {
		Map<String, byte[]> files = "null".equals(type) ? null : new HashMap<>();
		String sql = "SELECT * FROM read_parquet('upload://file1')";
		FireboltException exception = assertThrows(FireboltException.class,
				() -> parquetStatement.execute(sql, files));
		assertTrue(exception.getMessage().contains("Files map cannot be null or empty"));
	}

	@Test
	void shouldThrowExceptionWhenFileIdentifierIsNull() {
		String sql = "SELECT * FROM read_parquet('upload://file1')";
		Map<String, byte[]> files = new HashMap<>();
		files.put(null, new byte[]{1, 2, 3});
		FireboltException exception = assertThrows(FireboltException.class,
				() -> parquetStatement.execute(sql, files));
		assertTrue(exception.getMessage().contains("File identifier cannot be null"));
	}

	@Test
	void shouldThrowExceptionWhenFileContentIsNull() {
		String sql = "SELECT * FROM read_parquet('upload://file1')";
		Map<String, byte[]> files = new HashMap<>();
		files.put("file1", null);
		FireboltException exception = assertThrows(FireboltException.class,
				() -> parquetStatement.execute(sql, files));
		assertTrue(exception.getMessage().contains("File content for identifier 'file1' cannot be null"));
	}


	// Validation tests for validateSqlParameter
	@ParameterizedTest
	@NullSource
	@ValueSource(strings = {"", "   ", "\t", "\n"})
	void shouldThrowExceptionWhenSqlIsInvalid(String sql) {
		Map<String, byte[]> files = new HashMap<>();
		files.put("file1", testFileContent);
		FireboltException exception = assertThrows(FireboltException.class,
				() -> parquetStatement.execute(sql, files));
		assertTrue(exception.getMessage().contains("SQL cannot be null or blank"));
	}

	@Test
	void shouldThrowExceptionWhenStatementIsClosed() throws SQLException {
		parquetStatement.close();
		Map<String, byte[]> files = new HashMap<>();
		files.put("file1", testFileContent);
		FireboltException exception = assertThrows(FireboltException.class,
				() -> parquetStatement.execute("SELECT 1", files));
		assertTrue(exception.getMessage().contains("closed"));
	}

}

