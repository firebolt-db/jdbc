package com.firebolt.jdbc.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.FireboltException;

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
		Statement statement = mock(Statement.class);
		ResultSet resultSet = mock(ResultSet.class);
		when(resultSet.next()).thenReturn(true);
		when(resultSet.getString("status")).thenReturn("running");
		when(resultSet.getString("engine_url")).thenReturn("https://url");
		when(resultSet.getString("engine_name")).thenReturn("hello-engine");
		when(fireboltConnection.createSystemEngineStatementStatement()).thenReturn(statement);
		when(statement.executeQuery(any())).thenReturn(resultSet);
		assertEquals(Engine.builder().endpoint("https://url").name("hello-engine").build(),
				fireboltEngineService.getEngine(null, "db"));
	}

	@Test
	void shouldThrowExceptionWhenDefaultEngineNotRunning() throws SQLException {
		Statement statement = mock(Statement.class);
		ResultSet resultSet = mock(ResultSet.class);
		when(resultSet.next()).thenReturn(true);
		when(resultSet.getString("status")).thenReturn("down");
		when(fireboltConnection.createSystemEngineStatementStatement()).thenReturn(statement);
		when(statement.executeQuery(any())).thenReturn(resultSet);
		assertThrows(FireboltException.class, () -> fireboltEngineService.getEngine(null, "db"));
	}

	@Test
	void shouldGetEngineWhenEngineNameIsProvided() throws SQLException {
		Statement statement = mock(Statement.class);
		ResultSet resultSet = mock(ResultSet.class);
		when(resultSet.next()).thenReturn(true);
		when(resultSet.getString("status")).thenReturn("running");
		when(resultSet.getString("engine_url")).thenReturn("https://url");
		when(fireboltConnection.createSystemEngineStatementStatement()).thenReturn(statement);
		when(statement.executeQuery(any())).thenReturn(resultSet);
		assertEquals(Engine.builder().endpoint("https://url").name("some-engine").build(),
				fireboltEngineService.getEngine("some-engine", "db"));
	}

	@Test
	void shouldThrowExceptionWhenEngineNotRunning() throws SQLException {
		Statement statement = mock(Statement.class);
		ResultSet resultSet = mock(ResultSet.class);
		when(resultSet.next()).thenReturn(true);
		when(resultSet.getString("status")).thenReturn("down");
		when(fireboltConnection.createSystemEngineStatementStatement()).thenReturn(statement);
		when(statement.executeQuery(any())).thenReturn(resultSet);
		assertThrows(FireboltException.class, () -> fireboltEngineService.getEngine("some-engine", "db"));
	}

}
