package com.firebolt.jdbc.statement;

import com.firebolt.jdbc.CheckedBiFunction;
import com.firebolt.jdbc.client.query.StatementClient;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.connection.settings.FireboltSessionProperty;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.resultset.FireboltResultSet;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.rawstatement.QueryRawStatement;
import com.firebolt.jdbc.type.array.SqlArrayUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Wrapper;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.sql.Statement.CLOSE_CURRENT_RESULT;
import static java.sql.Statement.SUCCESS_NO_INFO;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

@ExtendWith(MockitoExtension.class)
class FireboltStatementTest {
    @Captor
    ArgumentCaptor<StatementInfoWrapper> queryInfoWrapperArgumentCaptor;
    @Mock
    private FireboltStatementService fireboltStatementService;
    @Mock
    private FireboltConnection fireboltConnection;

    private static FireboltStatement statement;
    private final FireboltProperties fireboltProperties = FireboltProperties.builder()
            .initialAdditionalProperties(new HashMap<>())
            .bufferSize(((Number)FireboltSessionProperty.BUFFER_SIZE.getDefaultValue()).intValue())
            .build();

    private static Stream<Arguments> unsupported() {
        return Stream.of(
                Arguments.of("setCursorName", (Executable) () -> statement.setCursorName("my_cursor")),
                Arguments.of("getGeneratedKeys", (Executable) () -> statement.getGeneratedKeys()),
                Arguments.of("executeUpdate(column indexes)", (Executable) () -> statement.executeUpdate("insert", new int[] {1})),
                Arguments.of("executeUpdate(column names)", (Executable) () -> statement.executeUpdate("insert", new String[] {"foo"})),
                Arguments.of("execute(auto generated keys)", (Executable) () -> statement.execute("insert", Statement.RETURN_GENERATED_KEYS)),
                Arguments.of("execute(column indexes)", (Executable) () -> statement.execute("insert", new int[] {1})),
                Arguments.of("execute(column names)", (Executable) () -> statement.execute("insert", new String[] {"foo"})),
                Arguments.of("execute(auto generated keys)", (Executable) () -> statement.execute("insert", Statement.RETURN_GENERATED_KEYS)),
                Arguments.of("setPoolable(true)", (Executable) () -> statement.setPoolable(true))
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unsupported")
    void shouldThrowSQLFeatureNotSupportedException(String name, Executable function) {
        statement = new FireboltStatement(fireboltStatementService, mock(FireboltProperties.class), mock(FireboltConnection.class));
        assertThrows(SQLFeatureNotSupportedException.class, function);
    }

    @Test
    void shouldExtractAdditionalPropertiesAndNotExecuteQueryWhenSetParamIsUsed() throws SQLException {
        FireboltConnection connection = mock(FireboltConnection.class);
        try (FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, connection)) {
            fireboltStatement.execute("set custom_1 = 1");
            verifyNoMoreInteractions(fireboltStatementService);
            verify(connection).addProperty(Map.entry("custom_1", "1"));
        }
    }

    @Test
    void shouldAbortStatementOnCancel() throws SQLException, ReflectiveOperationException {
        FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, fireboltConnection);

        Field runningStatementField = FireboltStatement.class.getDeclaredField("runningStatementLabel");
        runningStatementField.setAccessible(true);
        runningStatementField.set(fireboltStatement, "1234");
        fireboltStatement.cancel();
        verify(fireboltStatementService).abortStatement(any(), eq(fireboltProperties));
    }

    @Test
    void shouldCloseInputStreamOnClose() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        FireboltConnection connection = mock(FireboltConnection.class);
        FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, connection);

        when(fireboltStatementService.execute(any(), any(), any())).thenReturn(Optional.empty());
        when(fireboltStatementService.execute(any(), any(), any())).thenReturn(Optional.of(rs));
        fireboltStatement.executeQuery("show database");
        fireboltStatement.close();
        verify(rs).close();
        verify(connection).removeClosedStatement(fireboltStatement);

        // validate that recurrent close does not create cascading closing calls.
        fireboltStatement.close();
        verifyNoMoreInteractions(connection);
    }

    @Test
    void shouldThrowAnExceptionWhenExecutingQueryOnANonQueryStatement() throws SQLException {
        FireboltConnection connection = mock(FireboltConnection.class);
        try (FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, connection)) {
            assertThrows(FireboltException.class, () -> fireboltStatement.executeQuery("set custom_1 = 1"));
        }
    }

    @Test
    void shouldExecuteIfUpdateStatementWouldNotReturnAResultSet() throws SQLException {
        shouldExecuteIfUpdateStatementWouldNotReturnAResultSet(Statement::executeUpdate);
    }

    @Test
    void shouldExecuteIfUpdateStatementWouldNotReturnAResultSetNoGeneratedKeys() throws SQLException {
        shouldExecuteIfUpdateStatementWouldNotReturnAResultSet((fireboltStatement, sql) -> fireboltStatement.executeUpdate(sql, Statement.NO_GENERATED_KEYS));
    }

    @Test
    void shouldExecuteIfUpdateStatementWithIndexesWouldNotReturnAResultSet() throws SQLException {
        shouldExecuteIfUpdateStatementWouldNotReturnAResultSet((fireboltStatement, sql) -> fireboltStatement.executeUpdate(sql, new int[0]));
    }

    @Test
    void shouldExecuteIfUpdateStatementWithNamesWouldNotReturnAResultSet() throws SQLException {
        shouldExecuteIfUpdateStatementWouldNotReturnAResultSet((fireboltStatement, sql) -> fireboltStatement.executeUpdate(sql, new String[0]));
    }

    private void shouldExecuteIfUpdateStatementWouldNotReturnAResultSet(CheckedBiFunction<Statement, String, Integer> executor) throws SQLException {
        try (FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, fireboltConnection)) {
            when(fireboltStatementService.execute(any(), any(), any())).thenReturn(Optional.empty());
            assertEquals(0, executor.apply(fireboltStatement, "INSERT INTO cars(sales, name) VALUES (500, 'Ford')"));
            verify(fireboltStatementService).execute(queryInfoWrapperArgumentCaptor.capture(), eq(fireboltProperties), any());
            assertEquals("INSERT INTO cars(sales, name) VALUES (500, 'Ford')",
                    queryInfoWrapperArgumentCaptor.getValue().getSql());
            assertEquals(0, fireboltStatement.getUpdateCount());
        }
    }

    @Test
    void shouldExecuteStatementThatReturnsResultSet() throws SQLException {
        shouldExecuteStatementThatReturnsResultSet(Statement::execute);
    }

    @Test
    void shouldExecuteStatementNoGeneratedKeysThatReturnsResultSet() throws SQLException {
        shouldExecuteStatementThatReturnsResultSet((fireboltStatement, sql) -> fireboltStatement.execute(sql, Statement.NO_GENERATED_KEYS));
    }

    @Test
    void shouldExecuteStatementWithIndexesWouldNotReturnAResultSet() throws SQLException {
        shouldExecuteStatementThatReturnsResultSet((fireboltStatement, sql) -> fireboltStatement.execute(sql, new int[0]));
    }

    @Test
    void shouldExecuteStatementWithNamesWouldNotReturnAResultSet() throws SQLException {
        shouldExecuteStatementThatReturnsResultSet((fireboltStatement, sql) -> fireboltStatement.execute(sql, new String[0]));
    }


    private void shouldExecuteStatementThatReturnsResultSet(CheckedBiFunction<Statement, String, Boolean> executor) throws SQLException {
        ResultSet rs = mock(FireboltResultSet.class);
        FireboltConnection connection = mock(FireboltConnection.class);
        FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, connection);
        when(fireboltStatementService.execute(any(), any(), any())).thenReturn(Optional.of(rs));
        assertTrue(executor.apply(fireboltStatement, "SELECT 1"));
        assertEquals(rs, fireboltStatement.getResultSet());
        assertEquals(-1, fireboltStatement.getUpdateCount());
        assertFalse(fireboltStatement.getMoreResults());
        verify(rs).close();
        assertNull(fireboltStatement.getResultSet());
    }

    @Test
    void shouldCloseCurrentAndGetMoreResultsForMultiStatementQuery() throws SQLException {
        ResultSet rs = mock(FireboltResultSet.class);
        ResultSet rs2 = mock(FireboltResultSet.class);
        FireboltConnection connection = mock(FireboltConnection.class);
        FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, connection);

        when(fireboltStatementService.execute(any(), any(), any())).thenReturn(Optional.of(rs)).thenReturn(Optional.of(rs2));
        fireboltStatement.execute("SELECT 1; SELECT 2;");
        assertEquals(rs, fireboltStatement.getResultSet());
        assertEquals(-1, fireboltStatement.getUpdateCount());
        fireboltStatement.getMoreResults();
        verify(rs).close();
        assertEquals(rs2, fireboltStatement.getResultSet());
        rs = fireboltStatement.getResultSet();
        fireboltStatement.getMoreResults();
        verify(rs).close();
        assertEquals(-1, fireboltStatement.getUpdateCount());
        assertNull(fireboltStatement.getResultSet());
    }


    @Test
    void test() throws SQLException {
        FireboltConnection connection = mock(FireboltConnection.class);
        FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, connection);

        List<String> logMessages = new ArrayList<>();
        Logger log = Logger.getLogger(FireboltStatement.class.getName());
        log.setLevel(Level.ALL);
        log.addHandler(new Handler() {
            @Override
            public void publish(LogRecord record) {
                logMessages.add(new MessageFormat(record.getMessage()).format(record.getParameters()));
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() throws SecurityException {
            }
        });

        String query = "SELECT 1";
        // This trick simulates cancelled statement. getLabel() called first time returns the initial label generated
        // when StatementInfoWrapper is created. This value is stored in a collection of currently running queries.
        // But next invocation of getLabel() will return other value, so the statement will not be found in list of running
        // queries and will be considered as cancelled.
        // It is ugly trick, but I do not know better way to simulate cancelled query.
        StatementInfoWrapper statementInfoWrapper = new StatementInfoWrapper(query, StatementType.QUERY, null, new QueryRawStatement(query, query, List.of())) {
            String label = null;
            @Override
            public String getLabel() {
                if (label != null) {
                    return label;
                }
                String currentLabel = super.getLabel();
                label = "other label";
                return currentLabel;
            }
        };
        fireboltStatement.execute(List.of(statementInfoWrapper));
        assertNull(fireboltStatement.getResultSet());
        fireboltStatement.getMoreResults(CLOSE_CURRENT_RESULT);
        verify(fireboltStatementService, times(0)).execute(any(), any(), any());
        assertTrue(logMessages.contains("Aborted query with id other label"), "Expected log message is not found");
    }


    @Test
    void shouldCloseCurrentAndGetMoreResultWhenCallingGetMoreResultsWithCloseCurrentFlag() throws SQLException {
        FireboltConnection connection = mock(FireboltConnection.class);
        FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, connection);
        when(fireboltStatementService.execute(any(), any(), any())).thenReturn(Optional.of(mock(FireboltResultSet.class)));
        fireboltStatement.execute("SELECT 1; SELECT 2;");
        ResultSet resultSet = fireboltStatement.getResultSet();
        fireboltStatement.getMoreResults(CLOSE_CURRENT_RESULT);
        verify(resultSet).close();
    }

    @Test
    void shouldKeepCurrentAndGetMoreResultWhenCallingGetMoreResultsWithKeepCurrentResultFlag() throws SQLException {
        FireboltConnection connection = mock(FireboltConnection.class);
        FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, connection);
        when(fireboltStatementService.execute(any(), any(), any())).thenReturn(Optional.of(mock(ResultSet.class)));
        fireboltStatement.execute("SELECT 1; SELECT 2;");
        ResultSet resultSet = fireboltStatement.getResultSet();
        fireboltStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
        verify(resultSet, never()).close();
    }

    @Test
    void shouldCloseUnclosedAndGetMoreResultWhenCallingGetMoreResultsWithCloseAllResultFlag() throws SQLException {
        ResultSet rs = mock(FireboltResultSet.class);
        ResultSet rs2 = mock(FireboltResultSet.class);
        ResultSet rs3 = mock(FireboltResultSet.class);
        FireboltConnection connection = mock(FireboltConnection.class);
        FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, connection);

        when(fireboltStatementService.execute(any(), any(), any()))
                .thenReturn(Optional.of(rs)).thenReturn(Optional.of(rs2)).thenReturn(Optional.of(rs3));

        fireboltStatement.execute("SELECT 1; SELECT 2; SELECT 3;");
        ResultSet firstRs = fireboltStatement.getResultSet();
        fireboltStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
        verify(firstRs, never()).close();
        ResultSet secondRs = fireboltStatement.getResultSet();
        fireboltStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
        verify(secondRs, never()).close();
        fireboltStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS);
        verify(firstRs).close();
        verify(secondRs).close();

    }

    @Test
    void maxRows() throws SQLException {
        try (Statement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class))) {
            assertEquals(0, statement.getMaxRows()); // zero means there is no limit
            statement.setMaxRows(123);
            assertEquals(123, statement.getMaxRows());
        }
    }

    @Test
    void negativeMaxRows() throws SQLException {
        try (Statement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class))) {
            assertThrows(SQLException.class, () -> statement.setMaxRows(-1));
        }
    }

    @Test
    void statementParameters() throws SQLException {
        try (Statement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class))) {
            assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
            assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());
            assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());
            assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, statement.getResultSetHoldability());
            assertNull(statement.getWarnings());
        }
    }

    @Test
    void setEscapeProcessing() throws SQLException {
        try (Statement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class))) {
            assertNull(statement.getWarnings());
            statement.setEscapeProcessing(false);
            assertNull(statement.getWarnings());
            // set escape processing and get 1 warning
            statement.setEscapeProcessing(true);
            SQLWarning firstWarning = statement.getWarnings();
            assertNotNull(firstWarning);
            assertNull(firstWarning.getNextWarning());

            // now do it again. Now we get 2 warnings: first one - the same, and yet another one - chained
            statement.setEscapeProcessing(true);
            assertSame(firstWarning, statement.getWarnings());
            assertNotNull(firstWarning.getNextWarning());
            assertNull(firstWarning.getNextWarning().getNextWarning());

            // now disable escape processing. The warnings remain the same
            statement.setEscapeProcessing(false);
            assertSame(firstWarning, statement.getWarnings());
            assertNotNull(firstWarning.getNextWarning());
            assertNull(firstWarning.getNextWarning().getNextWarning());

            // clean warnings and validate them.
            statement.clearWarnings();
            assertNull(statement.getWarnings());
        }
    }

    @ParameterizedTest
    @ValueSource(classes = {Statement.class, FireboltStatement.class, Wrapper.class, AutoCloseable.class})
    void successfulUnwrap(Class<?> clazz) throws SQLException {
        Statement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
        assertSame(statement, statement.unwrap(clazz));
    }

    @ParameterizedTest
    @ValueSource(classes = {Connection.class, String.class, Closeable.class})
    void failingUnwrap(Class<?> clazz) {
        Statement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
        assertThrows(SQLException.class, () -> statement.unwrap(clazz));
    }

    @ParameterizedTest
    @ValueSource(classes = {Statement.class, FireboltStatement.class, Wrapper.class, AutoCloseable.class})
    void isWrapperFor(Class<?> clazz) throws SQLException {
        Statement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
        assertTrue(statement.isWrapperFor(clazz));
    }

    @ParameterizedTest
    @ValueSource(classes = {Connection.class, String.class, Closeable.class})
    void isNotWrapperFor(Class<?> clazz) throws SQLException {
        Statement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
        assertFalse(statement.isWrapperFor(clazz));
    }

    @Test
    void queryTimeout() throws SQLException {
        Statement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
        assertEquals(0, statement.getQueryTimeout());
        statement.setQueryTimeout(12345);
        assertEquals(12345, statement.getQueryTimeout());
    }

    @Test
    void closeOnCompletion() throws SQLException {
        Statement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
        assertFalse(statement.isCloseOnCompletion());
        statement.closeOnCompletion();
        assertTrue(statement.isCloseOnCompletion());
    }

    @Test
    void isPoolable() throws SQLException {
        Statement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
        assertFalse(statement.isPoolable());
        assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setPoolable(true));
        assertFalse(statement.isPoolable());
        assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setPoolable(false));
        assertFalse(statement.isPoolable());
    }

    @Test
    void fetchSize() throws SQLException {
        Statement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
        assertEquals(0, statement.getFetchSize());
        statement.setFetchSize(123); // ignore
        assertEquals(0, statement.getFetchSize());
        assertThrows(SQLException.class, () -> statement.setFetchSize(-1));
    }

    @Test
    void fetchDirection() throws SQLException {
        Statement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
        assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection()); // check initial value

        // set the same value; should succeed
        statement.setFetchDirection(ResultSet.FETCH_FORWARD);
        assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());

        // set wrong values expecting exceptions
        assertThrows(SQLException.class, () -> statement.setFetchDirection(ResultSet.FETCH_REVERSE));
        assertThrows(SQLException.class, () -> statement.setFetchDirection(ResultSet.FETCH_UNKNOWN));
        assertThrows(SQLException.class, () -> statement.setFetchDirection(999999));
        // check that returned value is still FETCH_FORWARD
        assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
    }

    @Test
    void maxFieldSize() throws SQLException {
        Statement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class));
        assertEquals(0, statement.getMaxFieldSize());
        statement.setMaxFieldSize(123);
        assertEquals(123, statement.getMaxFieldSize());
    }

    @Test
    void getConnection() throws SQLException {
        FireboltConnection connection = mock(FireboltConnection.class);
        Statement statement = new FireboltStatement(fireboltStatementService, null, connection);
        assertSame(connection, statement.getConnection());
    }

    @Test
    void warnings() throws SQLException {
        FireboltStatement statement = new FireboltStatement(fireboltStatementService, null, mock(FireboltConnection.class)) {
            {
                addWarning(new SQLWarning("test"));
            }
        };
        SQLWarning warning = statement.getWarnings();
        assertNotNull(warning);
        assertEquals("test", warning.getMessage());
        statement.clearWarnings();
        assertNull(statement.getWarnings());
    }

    @ParameterizedTest
    @CsvSource(value = {
            "hello,5,hello",
            "hello,6,hello",
            "hello,0,hello",
            "hello,-1,hello",
            "hello,4,hell",
            "hello,2,he",
            "hello,1,h",
            ",-1,",
            ",0,",
            ",1,",
    })
    void shouldLimitStringByMaxFieldSize(String inputText, int maxFieldSize, String expectedText) throws SQLException {
        FireboltConnection connection = mock(FireboltConnection.class);
        StatementClient statementClient = mock(StatementClient.class);
        String content = format("1\t2\ntext\tbytea\n%s\t%s", inputText == null ? "\\N" : inputText, inputText == null ? "\\N" : SqlArrayUtil.byteArrayToHexString(inputText.getBytes(), false));
        when(statementClient.executeSqlStatement(any(), any(), eq(false), anyInt())).thenReturn(new ByteArrayInputStream(content.getBytes()));
        FireboltStatementService statementService = new FireboltStatementService(statementClient);
        FireboltStatement fireboltStatement = new FireboltStatement(statementService, fireboltProperties, connection);
        when(connection.createStatement()).thenReturn(fireboltStatement);
        fireboltStatement.setMaxFieldSize(maxFieldSize);
        ResultSet rs = connection.createStatement().executeQuery("SELECT word from table");
        assertTrue(rs.next());

        assertEquals(expectedText, rs.getString(1));
        assertEquals(expectedText, rs.getObject(1));

        if (expectedText == null) {
            assertNull(rs.getString(2));
            assertNull(rs.getObject(2));
        } else {
            String expectedHexText = SqlArrayUtil.byteArrayToHexString(expectedText.getBytes(), false);
            assertEquals(expectedHexText, rs.getString(2));
            assertArrayEquals(expectedText.getBytes(), (byte[]) rs.getObject(2));
        }

        assertFalse(rs.next());
    }

    @Test
    void shouldExecuteEmptyBatch() throws SQLException {
        FireboltConnection connection = mock(FireboltConnection.class);
        FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, connection);
        assertArrayEquals(new int[0], fireboltStatement.executeBatch());
    }

    @Test
    void shouldExecuteBatch() throws SQLException {
        FireboltConnection connection = mock(FireboltConnection.class);
        FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, connection);
        when(fireboltStatementService.execute(any(), any(), any())).thenReturn(
                Optional.of(mock(FireboltResultSet.class)), Optional.of(mock(FireboltResultSet.class)), Optional.empty(), Optional.empty(), Optional.of(mock(FireboltResultSet.class))
        );

        fireboltStatement.addBatch("SELECT 1; SELECT 2;");
        fireboltStatement.addBatch("INSERT INTO PEOPLE (id, name) VALUES (1, 'Adam')");
        fireboltStatement.addBatch("INSERT INTO PEOPLE (id, name) VALUES (1, 'Eve')");
        fireboltStatement.addBatch("SELECT 3");

        int[] actual = fireboltStatement.executeBatch();
        assertArrayEquals(new int[] {0, 0, SUCCESS_NO_INFO, SUCCESS_NO_INFO, 0}, actual);
    }

    @Test
    void shouldClearBatch() throws SQLException {
        FireboltConnection connection = mock(FireboltConnection.class);
        FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, connection);
        when(fireboltStatementService.execute(any(), any(), any())).thenReturn(
                Optional.empty(), Optional.empty()
        );

        fireboltStatement.addBatch("SELECT 1; SELECT 2;");
        fireboltStatement.clearBatch();
        assertArrayEquals(new int[0], fireboltStatement.executeBatch());

        fireboltStatement.addBatch("INSERT INTO PEOPLE (id, name) VALUES (1, 'Adam')");
        fireboltStatement.addBatch("INSERT INTO PEOPLE (id, name) VALUES (1, 'Eve')");

        assertArrayEquals(new int[] {SUCCESS_NO_INFO, SUCCESS_NO_INFO}, fireboltStatement.executeBatch());
    }
}