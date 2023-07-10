package com.firebolt.jdbc.service;

import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltEngineServiceTest {

	@InjectMocks
	private FireboltEngineService fireboltEngineService;

	@Mock
	private FireboltConnection fireboltConnection;

	@Test
	void shouldGetEngineNameFromEngineHost() throws SQLException {
		assertEquals("myHost_345", fireboltEngineService.getEngineNameByHost("myHost-345.firebolt.io"));
	}

	@Test
	void shouldThrowExceptionWhenThEngineCannotBeEstablishedFromTheHost() {
		assertThrows(FireboltException.class, () -> fireboltEngineService.getEngineNameByHost("myHost-345"));
	}

	@Test
	void shouldThrowExceptionWhenThEngineCannotBeEstablishedFromNullHost() {
		assertThrows(FireboltException.class, () -> fireboltEngineService.getEngineNameByHost(null));
	}

	@Test
	void shouldGetDefaultEngineWhenEngineNameIsNotProvided() throws SQLException {
		PreparedStatement statement = mock(PreparedStatement.class);
		ResultSet resultSet = mockedResultSet(Map.of("status", "running", "url", "https://url", "engine_name", "hello-engine"));
		when(fireboltConnection.prepareStatement(anyString())).thenReturn(statement);
		when(statement.executeQuery()).thenReturn(resultSet);
		assertEquals(Engine.builder().endpoint("https://url").name("hello-engine").build(),
				fireboltEngineService.getEngine(null, "db"));
	}

	@Test
	void shouldGetEngineWhenEngineNameIsProvided() throws SQLException {
		PreparedStatement statement = mock(PreparedStatement.class);
		ResultSet resultSet = mockedResultSet(Map.of("status", "running", "url", "https://url"));
		when(fireboltConnection.prepareStatement(anyString())).thenReturn(statement);
		when(statement.executeQuery()).thenReturn(resultSet);
		assertEquals(Engine.builder().endpoint("https://url").name("some-engine").build(),
				fireboltEngineService.getEngine("some-engine", "db"));
	}

	@ParameterizedTest
	@CsvSource(value = {
			"down;some-engine;db;SELECT url, status",
			"down;;db;SELECT.+JOIN",
			"failed;'';db;SELECT.+JOIN",
	}, delimiter = ';')
	void shouldThrowExceptionWhenEngineNotRunning(String status, String engineName, String db, String queryRegex) throws SQLException {
		PreparedStatement statement = mock(PreparedStatement.class);
		ResultSet resultSet = mockedResultSet(Map.of("status", status));
		when(fireboltConnection.prepareStatement(Mockito.matches(Pattern.compile(queryRegex, Pattern.MULTILINE | Pattern.DOTALL)))).thenReturn(statement);
		when(statement.executeQuery()).thenReturn(resultSet);
		assertThrows(FireboltException.class, () -> fireboltEngineService.getEngine(engineName, db));
	}

	private ResultSet mockedResultSet(Map<String, String> values) throws SQLException {
		ResultSet resultSet = mock(ResultSet.class);
		when(resultSet.next()).thenReturn(true);
		for (Entry<String, String> column : values.entrySet()) {
			when(resultSet.getString(column.getKey())).thenReturn(column.getValue());
		}
		return resultSet;
	}
}
