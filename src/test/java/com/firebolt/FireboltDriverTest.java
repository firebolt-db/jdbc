package com.firebolt;

import com.firebolt.jdbc.connection.FireboltConnection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockConstruction;

class FireboltDriverTest {
	private static final String PASSWORD_FIELD = "password";

	@ParameterizedTest
	@ValueSource(strings = {"invalid:url", "''", ""})
	void shouldNotReturnNewConnectionWhenUrlIsInvalid(String url) throws SQLException {
		assertNull(new FireboltDriver().connect(url, null));
	}

	@Test
	void shouldReturnNewConnectionWhenUrlIsValid() throws SQLException {
		try (MockedConstruction<FireboltConnection> mocked = mockConstruction(FireboltConnection.class)) {
			FireboltDriver fireboltDriver = new FireboltDriver();
			assertNotNull(fireboltDriver.connect("jdbc:firebolt://api.dev.firebolt.io/db_name", new Properties()));
			assertEquals(1, mocked.constructed().size());
		}
	}

	@ParameterizedTest
	@CsvSource(value = {"jdbc:firebolt,true", "'',false", ",false", "jdbc:mysql,false"}, delimiter = ',')
	void acceptsURL(String url) {
		new  FireboltDriver().acceptsURL(url);
	}

	@Test
	void getParentLogger() {
		assertThrows(SQLFeatureNotSupportedException.class, () -> new FireboltDriver().getParentLogger());
	}

	@Test
	void jdbcCompliant() {
		assertFalse(new FireboltDriver().jdbcCompliant()); // not yet... :(
	}

	@Test
	void version() {
		FireboltDriver fireboltDriver = new FireboltDriver();
		assertEquals(2, fireboltDriver.getMajorVersion());
		assertEquals(4, fireboltDriver.getMinorVersion());
	}

	@ParameterizedTest
	@CsvSource(value =
			{
					"jdbc:firebolt,,",
					"jdbc:firebolt://api.dev.firebolt.io/db_name,,host=api.dev.firebolt.io;path=/db_name",
					"jdbc:firebolt://api.dev.firebolt.io/db_name?account=test,,host=api.dev.firebolt.io;path=/db_name;account=test",
					"jdbc:firebolt://api.dev.firebolt.io/db_name?account=test,user=usr;" + PASSWORD_FIELD + "=pwd,host=api.dev.firebolt.io;path=/db_name;account=test;user=usr;" + PASSWORD_FIELD + "=pwd",
					"jdbc:firebolt://api.dev.firebolt.io/db_name,user=usr;" + PASSWORD_FIELD + "=pwd,host=api.dev.firebolt.io;path=/db_name;user=usr;" + PASSWORD_FIELD + "=pwd"
			},
			delimiter = ',')
	void getPropertyInfo(String url, String propStr, String expectedInfoStr) throws SQLException {
		Properties expectedProps = toProperties(expectedInfoStr);
		assertEquals(expectedProps, toMap(new FireboltDriver().getPropertyInfo(url, toProperties(propStr))).entrySet().stream().filter(e -> expectedProps.containsKey(e.getKey())).collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
	}

	private Map<String, String> toMap(DriverPropertyInfo[] prop) {
		return Arrays.stream(prop).collect(Collector.of(
				HashMap::new, (properties, kv) -> properties.put(kv.name, kv.value),
				(one, two) -> {
					one.putAll(two);
					return one;
				}
		));
	}

	private Properties toProperties(String str) {
		return str == null ? new Properties() : Arrays.stream(str.split("\\s*;\\s*")).map(kv -> kv.split("\\s*=\\s*")).collect(Collector.of(
				Properties::new, (properties, strings) -> properties.setProperty(strings[0], strings[1]),
				(one, two) -> {
					one.putAll(two);
					return one;
				}
		));
	}
}
