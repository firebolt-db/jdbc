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
	@CsvSource(value = {
			"jdbc:firebolt://api.dev.firebolt.io/db_name,true",
			"'',false",
			",false",
			"jdbc:mysql://host:3306/db,false"
	}, delimiter = ',')
	void acceptsURL(String url, boolean expected) {
		assertEquals(expected, new  FireboltDriver().acceptsURL(url));
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
		assertEquals(3, fireboltDriver.getMajorVersion());
		assertEquals(0, fireboltDriver.getMinorVersion());
	}

	@ParameterizedTest
	@CsvSource(value =
			{
					"jdbc:firebolt,,",
					"jdbc:firebolt:db_name,,environment=app;path=db_name",
					"jdbc:firebolt:db_name?account=test,,environment=app;path=db_name;account=test",
					"jdbc:firebolt:db_name?account=test,client_id=usr;client_secret=pwd,environment=app;path=db_name;account=test;client_id=usr;client_secret=pwd",
					"jdbc:firebolt:db_name,client_id=usr;client_secret=pwd,environment=app;path=db_name;client_id=usr;client_secret=pwd"
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
