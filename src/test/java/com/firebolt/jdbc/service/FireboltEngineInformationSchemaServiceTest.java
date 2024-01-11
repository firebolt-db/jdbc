package com.firebolt.jdbc.service;

import com.firebolt.jdbc.connection.Engine;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import static java.lang.String.format;
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
	@CsvSource({
			/*in:*/ "db,running,my-url,my-url," + /*expected:*/  "some-engine,db,",
			/*in:*/ ",running,api.region.env.firebolt.io,api.region.env.firebolt.io," +  /*expected:*/ "some-engine,,",
			/*in:*/ "db,ENGINE_STATE_RUNNING,api.us-east-1.dev.firebolt.io?account_id=01hf9pchg0mnrd2g3hypm1dea4&engine=max_test,api.us-east-1.dev.firebolt.io," + /*expected:*/ "max_test,db,01hf9pchg0mnrd2g3hypm1dea4",
	})
	void shouldGetEngineWhenEngineNameIsProvided(String db, String engineStatus, String engineUrl, String expectedEngineUrl, String expectedEngine, String expectedDb, String expectedAccountId) throws SQLException {
		PreparedStatement statement = mock(PreparedStatement.class);
		ResultSet resultSet = mockedResultSet(Map.of("status", engineStatus, "url", engineUrl, "attached_to", "db", "engine_name", "some-engine"));
		when(fireboltConnection.prepareStatement(anyString())).thenReturn(statement);
		when(statement.executeQuery()).thenReturn(resultSet);
		FireboltProperties properties = createFireboltProperties("some-engine", db);
		assertEquals(new Engine(expectedEngineUrl, engineStatus, "some-engine", "db", null), fireboltEngineService.getEngine(properties));
		assertEquals(expectedEngine, properties.getEngine());
		assertEquals(expectedDb, properties.getDatabase());
		assertEquals(expectedAccountId, properties.getAccountId());
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
	@CsvSource(value = {
			"mydb;'';'';;false", // database is not found in databases table and catalogs table does not exist
			"mydb;'';'table_name,catalogs';'';false", // database is not found in databases table; catalogs table exists but needed database is not there
			"mydb;'';'table_name,catalogs';'catalog_name,your_db';true", // database is not found in databases table but is found in catalogs table
			"other_db;'database_name,other_db';;;true" // database is found in databases table
	}, delimiter = ';')
	void doesDatabaseExist(String db, String dbRow, String tableRow, String catalogRow, boolean expected) throws SQLException {
		PreparedStatement dbStatement = mockedEntityStatement("database", dbRow);
		PreparedStatement tableStatement = mockedEntityStatement("table", tableRow);
		PreparedStatement catalogStatement = mockedEntityStatement("catalog", catalogRow);
		assertEquals(expected, fireboltEngineService.doesDatabaseExist(db));
		Mockito.verify(dbStatement, Mockito.times(1)).setString(1, db);
		if (tableStatement != null) {
			Mockito.verify(tableStatement, Mockito.times(1)).setString(1, "catalogs");
		}
		if (catalogStatement != null) {
			Mockito.verify(catalogStatement, Mockito.times(1)).setString(1, db);
		}
	}

	private PreparedStatement mockedEntityStatement(String entity, String row) throws SQLException {
		if (row == null) {
			return null;
		}
		PreparedStatement statement = mock(PreparedStatement.class);
		ResultSet resultSet = mockedResultSet(row.isEmpty() ? Map.of() : Map.of(row.split(",")[0], row.split(",")[1]));
		when(fireboltConnection.prepareStatement(format("SELECT * FROM information_schema.%ss WHERE %s_name=?", entity, entity))).thenReturn(statement);
		when(statement.executeQuery()).thenReturn(resultSet);
		return statement;
	}

	private ResultSet mockedResultSet(Map<String, String> values) throws SQLException {
		ResultSet resultSet = mock(ResultSet.class);
		if (values == null || values.isEmpty()) {
			when(resultSet.next()).thenReturn(false);
		} else {
			when(resultSet.next()).thenReturn(true);
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
