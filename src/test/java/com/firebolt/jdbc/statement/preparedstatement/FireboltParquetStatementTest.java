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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
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
	private FireboltStatementService statementService;
	@Mock
	private FireboltConnection connection;
	@Mock
	private FireboltProperties sessionProperties;
	@Mock
	private StatementValidator statementValidator;
	@Mock
	private FireboltResultSet resultSet;

	@Captor
	private ArgumentCaptor<StatementInfoWrapper> statementInfoWrapperCaptor;
	@Captor
	private ArgumentCaptor<Map<String, byte[]>> filesCaptor;

	@TempDir
	Path tempDir;

	private FireboltParquetStatement parquetStatement;
	private byte[] testFileContent;

	@BeforeEach
	void setUp() {
		when(connection.getSessionProperties()).thenReturn(sessionProperties);
		parquetStatement = new FireboltParquetStatement(statementService, connection);
		testFileContent = "test parquet content".getBytes();
	}

	@Test
	void shouldThrowExceptionWhenCallingExecuteQueryWithStringOnly() {
		FireboltException exception = assertThrows(FireboltException.class,
				() -> parquetStatement.executeQuery("SELECT 1"));
		assertTrue(exception.getMessage().contains("Cannot call executeQuery(String sql)"));
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

	@ParameterizedTest(name = "{0}")
	@MethodSource("invalidSqlOrFilesArguments")
	void shouldThrowExceptionForInvalidSqlOrFiles(String testName, String sql, Map<String, byte[]> files, String expectedErrorMessage) {
		FireboltException exception = assertThrows(FireboltException.class,
				() -> parquetStatement.executeQuery(sql, files));
		assertTrue(exception.getMessage().contains(expectedErrorMessage));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("invalidFilesMapArguments")
	void shouldThrowExceptionForInvalidFilesMap(String testName, Map<String, byte[]> files, String expectedErrorMessage) {
		FireboltException exception = assertThrows(FireboltException.class,
				() -> parquetStatement.executeQuery("SELECT 1", files));
		assertTrue(exception.getMessage().contains(expectedErrorMessage));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("invalidInputStreamsMapArguments")
	void shouldThrowExceptionForInvalidInputStreamsMap(String testName, Map<String, InputStream> inputStreams, String expectedErrorMessage) {
		FireboltException exception = assertThrows(FireboltException.class,
				() -> parquetStatement.executeQueryWithInputStreams("SELECT 1", inputStreams));
		assertTrue(exception.getMessage().contains(expectedErrorMessage));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("invalidFileMapArguments")
	void shouldThrowExceptionForInvalidFileMap(String testName, Map<String, File> fileMap, String expectedErrorMessage) throws IOException {
		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			FireboltException exception = assertThrows(FireboltException.class,
					() -> parquetStatement.executeQueryWithFiles("SELECT 1", fileMap));
			assertTrue(exception.getMessage().contains(expectedErrorMessage));
		}
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("fileValidationErrorArguments")
	void shouldThrowExceptionForFileValidationErrors(String testName, String expectedErrorMessage) throws Exception {
		Map<String, File> fileMap = createFileMapForValidation(testName);
		
		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			FireboltException exception = assertThrows(FireboltException.class,
					() -> parquetStatement.executeQueryWithFiles("SELECT 1", fileMap));
			assertTrue(exception.getMessage().contains(expectedErrorMessage));
		}
	}

	@Test
	void shouldThrowExceptionForStatementStateErrors() throws SQLException {
		Map<String, byte[]> files = new HashMap<>();
		files.put("file1", testFileContent);

		parquetStatement.close();
		FireboltException exception = assertThrows(FireboltException.class,
				() -> parquetStatement.executeQuery("SELECT 1", files));
		assertTrue(exception.getMessage().contains("closed"));
	}


	@Test
	void shouldExecuteQueryWithByteArrays() throws SQLException {
		String sql = "SELECT id, name FROM read_parquet('upload://file1')";
		Map<String, byte[]> files = new HashMap<>();
		files.put("file1", testFileContent);

		when(statementService.executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.of(resultSet));

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			ResultSet rs = parquetStatement.executeQuery(sql, files);

			assertNotNull(rs);
			assertEquals(resultSet, rs);
			verify(statementService).executeWithFiles(statementInfoWrapperCaptor.capture(), eq(sessionProperties), eq(parquetStatement), filesCaptor.capture());
			assertEquals("file1", filesCaptor.getValue().keySet().iterator().next());
		}
	}

	@Test
	void shouldExecuteQueryWithInputStreams() throws SQLException {
		String sql = "SELECT id, name FROM read_parquet('upload://file1')";
		Map<String, InputStream> inputStreams = new HashMap<>();
		inputStreams.put("file1", new ByteArrayInputStream(testFileContent));

		when(statementService.executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.of(resultSet));

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			ResultSet rs = parquetStatement.executeQueryWithInputStreams(sql, inputStreams);

			assertNotNull(rs);
			assertEquals(resultSet, rs);
			verify(statementService).executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), filesCaptor.capture());
			Map<String, byte[]> capturedFiles = filesCaptor.getValue();
			assertEquals(1, capturedFiles.size());
			assertTrue(capturedFiles.containsKey("file1"));
			assertArrayEquals(testFileContent, capturedFiles.get("file1"));
		}
	}

	@Test
	void shouldExecuteQueryWithFiles() throws SQLException, IOException {
		String sql = "SELECT id, name FROM read_parquet('upload://file1')";
		File testFile = tempDir.resolve("test.parquet").toFile();
		try (FileOutputStream fos = new FileOutputStream(testFile)) {
			fos.write(testFileContent);
		}

		Map<String, File> fileMap = new HashMap<>();
		fileMap.put("file1", testFile);

		when(statementService.executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.of(resultSet));

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			ResultSet rs = parquetStatement.executeQueryWithFiles(sql, fileMap);

			assertNotNull(rs);
			assertEquals(resultSet, rs);
			verify(statementService).executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), filesCaptor.capture());
			Map<String, byte[]> capturedFiles = filesCaptor.getValue();
			assertEquals(1, capturedFiles.size());
			assertTrue(capturedFiles.containsKey("file1"));
			assertArrayEquals(testFileContent, capturedFiles.get("file1"));
		}
	}

	@Test
	void shouldExecuteWithByteArrays() throws SQLException {
		String sql = "SELECT id, name FROM read_parquet('upload://file1')";
		Map<String, byte[]> files = new HashMap<>();
		files.put("file1", testFileContent);

		when(statementService.executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.of(resultSet));

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			boolean hasResultSet = parquetStatement.execute(sql, files);

			assertTrue(hasResultSet);
			verify(statementService).executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap());
		}
	}

	@Test
	void shouldExecuteWithInputStreams() throws SQLException {
		String sql = "SELECT id, name FROM read_parquet('upload://file1')";
		Map<String, InputStream> inputStreams = new HashMap<>();
		inputStreams.put("file1", new ByteArrayInputStream(testFileContent));

		when(statementService.executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.of(resultSet));

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			boolean hasResultSet = parquetStatement.executeWithInputStreams(sql, inputStreams);

			assertTrue(hasResultSet);
			verify(statementService).executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap());
		}
	}

	@Test
	void shouldExecuteWithFiles() throws SQLException, IOException {
		String sql = "SELECT id, name FROM read_parquet('upload://file1')";
		File testFile = tempDir.resolve("test.parquet").toFile();
		try (FileOutputStream fos = new FileOutputStream(testFile)) {
			fos.write(testFileContent);
		}

		Map<String, File> fileMap = new HashMap<>();
		fileMap.put("file1", testFile);

		when(statementService.executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.of(resultSet));

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			boolean hasResultSet = parquetStatement.executeWithFiles(sql, fileMap);

			assertTrue(hasResultSet);
			verify(statementService).executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap());
		}
	}

	@Test
	void shouldExecuteUpdateWithByteArrays() throws SQLException {
		String sql = "INSERT INTO test SELECT id, name FROM read_parquet('upload://file1')";
		Map<String, byte[]> files = new HashMap<>();
		files.put("file1", testFileContent);

		when(statementService.executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.empty());

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			parquetStatement.executeUpdate(sql, files);

			verify(statementService).executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap());
		}
	}

	@Test
	void shouldExecuteUpdateWithInputStreams() throws SQLException {
		String sql = "INSERT INTO test SELECT id, name FROM read_parquet('upload://file1')";
		Map<String, InputStream> inputStreams = new HashMap<>();
		inputStreams.put("file1", new ByteArrayInputStream(testFileContent));

		when(statementService.executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.empty());

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			parquetStatement.executeUpdateWithInputStreams(sql, inputStreams);

			verify(statementService).executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap());
		}
	}

	@Test
	void shouldExecuteUpdateWithFiles() throws SQLException, IOException {
		String sql = "INSERT INTO test SELECT id, name FROM read_parquet('upload://file1')";
		File testFile = tempDir.resolve("test.parquet").toFile();
		try (FileOutputStream fos = new FileOutputStream(testFile)) {
			fos.write(testFileContent);
		}

		Map<String, File> fileMap = new HashMap<>();
		fileMap.put("file1", testFile);

		when(statementService.executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.empty());

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			parquetStatement.executeUpdateWithFiles(sql, fileMap);

			verify(statementService).executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap());
		}
	}

	@Test
	void shouldHandleIOExceptionWhenReadingInputStream() throws SQLException {
		String sql = "SELECT 1";
		Map<String, InputStream> inputStreams = new HashMap<>();
		InputStream failingStream = new InputStream() {
			@Override
			public int read() throws IOException {
				throw new IOException("Test IO exception");
			}
		};
		inputStreams.put("file1", failingStream);

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			SQLException exception = assertThrows(SQLException.class,
					() -> parquetStatement.executeQueryWithInputStreams(sql, inputStreams));
			assertTrue(exception.getMessage().contains("Failed to read input stream"));
			assertTrue(exception.getMessage().contains("file1"));
		}
	}

	@Test
	void shouldHandleIOExceptionWhenReadingFile() throws SQLException, IOException {
		String sql = "SELECT 1";
		File testFile = tempDir.resolve("test.parquet").toFile();
		testFile.createNewFile();
		// Make file unreadable by deleting it after creation
		testFile.delete();

		Map<String, File> fileMap = new HashMap<>();
		fileMap.put("file1", testFile);

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			// This should fail at the file existence check, not IO
			FireboltException exception = assertThrows(FireboltException.class,
					() -> parquetStatement.executeQueryWithFiles(sql, fileMap));
			assertTrue(exception.getMessage().contains("File does not exist"));
		}
	}

	@Test
	void shouldConvertMultipleInputStreamsToBytes() throws SQLException {
		String sql = "SELECT 1";
		Map<String, InputStream> inputStreams = new HashMap<>();
		byte[] content1 = "content1".getBytes();
		byte[] content2 = "content2".getBytes();
		inputStreams.put("file1", new ByteArrayInputStream(content1));
		inputStreams.put("file2", new ByteArrayInputStream(content2));

		when(statementService.executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.of(resultSet));

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			parquetStatement.executeQueryWithInputStreams(sql, inputStreams);

			verify(statementService).executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), filesCaptor.capture());
			Map<String, byte[]> capturedFiles = filesCaptor.getValue();
			assertEquals(2, capturedFiles.size());
			assertArrayEquals(content1, capturedFiles.get("file1"));
			assertArrayEquals(content2, capturedFiles.get("file2"));
		}
	}

	@Test
	void shouldConvertMultipleFilesToBytes() throws SQLException, IOException {
		String sql = "SELECT 1";
		byte[] content1 = "content1".getBytes();
		byte[] content2 = "content2".getBytes();

		File file1 = tempDir.resolve("file1.parquet").toFile();
		File file2 = tempDir.resolve("file2.parquet").toFile();
		try (FileOutputStream fos1 = new FileOutputStream(file1);
			 FileOutputStream fos2 = new FileOutputStream(file2)) {
			fos1.write(content1);
			fos2.write(content2);
		}

		Map<String, File> fileMap = new HashMap<>();
		fileMap.put("file1", file1);
		fileMap.put("file2", file2);

		when(statementService.executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.of(resultSet));

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			parquetStatement.executeQueryWithFiles(sql, fileMap);

			verify(statementService).executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), filesCaptor.capture());
			Map<String, byte[]> capturedFiles = filesCaptor.getValue();
			assertEquals(2, capturedFiles.size());
			assertArrayEquals(content1, capturedFiles.get("file1"));
			assertArrayEquals(content2, capturedFiles.get("file2"));
		}
	}

	@Test
	void shouldThrowExceptionWhenQueryReturnsNoResultSet() throws SQLException {
		String sql = "SELECT 1";
		Map<String, byte[]> files = new HashMap<>();
		files.put("file1", testFileContent);

		when(statementService.executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.empty());

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			FireboltException exception = assertThrows(FireboltException.class,
					() -> parquetStatement.executeQuery(sql, files));
			assertTrue(exception.getMessage().contains("Could not return ResultSet"));
		}
	}

	@Test
	void shouldExecuteWithNoResultSet() throws SQLException {
		String sql = "INSERT INTO test VALUES (1)";
		Map<String, byte[]> files = new HashMap<>();
		files.put("file1", testFileContent);

		when(statementService.executeWithFiles(any(StatementInfoWrapper.class), eq(sessionProperties), eq(parquetStatement), anyMap()))
				.thenReturn(Optional.empty());

		try (MockedStatic<com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory> validatorFactory = mockStatic(com.firebolt.jdbc.statement.rawstatement.StatementValidatorFactory.class)) {
			validatorFactory.when(() -> createValidator(any(), eq(connection))).thenReturn(statementValidator);
			doNothing().when(statementValidator).validate(any());

			boolean hasResultSet = parquetStatement.execute(sql, files);

			assertFalse(hasResultSet);
		}
	}

    private static Stream<Arguments> invalidSqlOrFilesArguments() {
        byte[] testContent = "test content".getBytes();
        Map<String, byte[]> validFiles = new HashMap<>();
        validFiles.put("file1", testContent);

        return Stream.of(
                Arguments.of("SQL is null", null, validFiles, "SQL cannot be null or blank"),
                Arguments.of("SQL is blank", "   ", validFiles, "SQL cannot be null or blank"),
                Arguments.of("Files map is null", "SELECT 1", null, "Files map cannot be null or empty")
        );
    }

    private static Stream<Arguments> invalidFilesMapArguments() {
        byte[] testContent = "test content".getBytes();
        Map<String, byte[]> emptyFiles = new HashMap<>();
        Map<String, byte[]> nullIdentifier = new HashMap<>();
        nullIdentifier.put(null, testContent);
        Map<String, byte[]> nullContent = new HashMap<>();
        nullContent.put("file1", null);

        return Stream.of(
                Arguments.of("Files map is empty", emptyFiles, "Files map cannot be null or empty"),
                Arguments.of("File identifier is null", nullIdentifier, "File identifier cannot be null"),
                Arguments.of("File content is null", nullContent, "File content for identifier 'file1' cannot be null")
        );
    }

    private static Stream<Arguments> invalidInputStreamsMapArguments() {
        byte[] testContent = "test content".getBytes();
        Map<String, InputStream> emptyStreams = new HashMap<>();
        Map<String, InputStream> nullIdentifier = new HashMap<>();
        nullIdentifier.put(null, new ByteArrayInputStream(testContent));
        Map<String, InputStream> nullStream = new HashMap<>();
        nullStream.put("file1", null);

        return Stream.of(
                Arguments.of("InputStreams map is null", null, "Input streams map cannot be null or empty"),
                Arguments.of("InputStreams map is empty", emptyStreams, "Input streams map cannot be null or empty"),
                Arguments.of("InputStream identifier is null", nullIdentifier, "File identifier cannot be null"),
                Arguments.of("InputStream is null", nullStream, "Input stream for identifier 'file1' cannot be null")
        );
    }

    private static Stream<Arguments> invalidFileMapArguments() {
        Map<String, File> emptyFileMap = new HashMap<>();
        Map<String, File> nullFile = new HashMap<>();
        nullFile.put("file1", null);

        return Stream.of(
                Arguments.of("File map is null", null, "File map cannot be null or empty"),
                Arguments.of("File map is empty", emptyFileMap, "File map cannot be null or empty"),
                Arguments.of("File is null", nullFile, "File for identifier 'file1' cannot be null")
        );
    }

    private static Stream<Arguments> fileValidationErrorArguments() {
        return Stream.of(
                Arguments.of("File identifier is null in FileMap", "File identifier cannot be null"),
                Arguments.of("File does not exist", "File does not exist"),
                Arguments.of("Path is not a file", "Path is not a file")
        );
    }

    private Map<String, File> createFileMapForValidation(String testName) throws IOException {
        Map<String, File> fileMap = new HashMap<>();

        switch (testName) {
            case "File identifier is null in FileMap":
                File testFile = tempDir.resolve("test.parquet").toFile();
                testFile.createNewFile();
                fileMap.put(null, testFile);
                break;
            case "File does not exist":
                File nonExistentFile = tempDir.resolve("nonexistent.parquet").toFile();
                fileMap.put("file1", nonExistentFile);
                break;
            case "Path is not a file":
                fileMap.put("file1", tempDir.toFile());
                break;
            default:
                throw new IllegalArgumentException("Unknown test name: " + testName);
        }

        return fileMap;
    }

	// Helper method for assertArrayEquals
	private static void assertArrayEquals(byte[] expected, byte[] actual) {
		org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual);
	}
}

