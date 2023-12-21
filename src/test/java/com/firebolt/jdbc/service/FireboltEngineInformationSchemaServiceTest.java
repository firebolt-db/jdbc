package com.firebolt.jdbc.service;

import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltEngineInformationSchemaServiceTest {

	@InjectMocks
	private FireboltEngineInformationSchemaService fireboltEngineService;

	@Mock
	private FireboltConnection fireboltConnection;

	@Test
	void shouldThrowExceptionEngineWhenEngineNameIsNotProvided() {
		FireboltProperties properties = FireboltProperties.builder().database("db").build();
		assertThrows(IllegalArgumentException.class, () -> fireboltEngineService.getEngine(properties));
	}

	@ParameterizedTest
	@ValueSource(strings = "db")
	@NullSource
	void shouldGetEngineWhenEngineNameIsProvided(String db) throws SQLException {
		PreparedStatement statement = mock(PreparedStatement.class);
		ResultSet resultSet = mockedResultSet(Map.of("status", "running", "url", "https://url", "attached_to", "db", "engine_name", "some-engine"));
		when(fireboltConnection.prepareStatement(anyString())).thenReturn(statement);
		when(statement.executeQuery()).thenReturn(resultSet);
		assertEquals(new Engine("https://url", "running", "some-engine", "db", null, Collections.emptyList()), fireboltEngineService.getEngine(createFireboltProperties("some-engine", "db")));
	}

	@ParameterizedTest
	@CsvSource(value = {
			"engine1;db1;http://url1;running;;The engine with the name engine1 is not attached to any database",
			"engine1;db1;http://url1;running;db2;The engine with the name engine1 is not attached to database db1",
			"engine1;db1;http://url1;starting;;The engine with the name engine1 is not running. Status: starting",
			"engine2;;;;;The engine with the name engine2 could not be found",
	}, delimiter = ';')
	void shouldThrowExceptionWhenSomethingIsWrong(String engineName, String db, String endpoint, String status, String attachedDb, String errorMessage) throws SQLException {
		PreparedStatement statement = mock(PreparedStatement.class);
		Map<String, String> rsData = null;
		if (endpoint != null || status != null || attachedDb != null) {
			rsData = new HashMap<>();
			rsData.put("url", endpoint);
			rsData.put("status", status);
			rsData.put("attached_to", attachedDb);
			rsData.put("engine_name", engineName);
		}
		ResultSet resultSet = mockedResultSet(rsData);
		when(fireboltConnection.prepareStatement(Mockito.matches(Pattern.compile("SELECT.+JOIN", Pattern.MULTILINE | Pattern.DOTALL)))).thenReturn(statement);
		when(statement.executeQuery()).thenReturn(resultSet);
		assertEquals(errorMessage, assertThrows(FireboltException.class, () -> fireboltEngineService.getEngine(createFireboltProperties(engineName, db))).getMessage());
		Mockito.verify(statement, Mockito.times(1)).setString(1, engineName);
	}

	@ParameterizedTest
	@CsvSource(value = {"mydb;'';false", "other_db;'database_name,other_db';true"}, delimiter = ';')
	void doesDatabaseExist(String db, String row, boolean expected) throws SQLException {
		PreparedStatement statement = mock(PreparedStatement.class);
		Map<String, String> rowData = row == null || row.isEmpty() ? Map.of() : Map.of(row.split(",")[0], row.split(",")[1]);
		ResultSet resultSet = mockedResultSet(rowData);
		when(fireboltConnection.prepareStatement("SELECT database_name FROM information_schema.databases WHERE database_name=?")).thenReturn(statement);
		when(statement.executeQuery()).thenReturn(resultSet);
		assertEquals(expected, fireboltEngineService.doesDatabaseExist(db));
		Mockito.verify(statement, Mockito.times(1)).setString(1, db);
	}

	private ResultSet mockedResultSet(Map<String, String> values) throws SQLException {
		ResultSet resultSet = mock(ResultSet.class);
		if (values == null || values.isEmpty()) {
			when(resultSet.next()).thenReturn(false);
		} else {
			when(resultSet.next()).thenReturn(true, false);
			for (Entry<String, String> column : values.entrySet()) {
				lenient().when(resultSet.getString(column.getKey())).thenReturn(column.getValue());
			}
		}
		return resultSet;
	}

	private FireboltProperties createFireboltProperties(String engine, String database) {
		return FireboltProperties.builder().engine(engine).database(database).build();
	}
}
