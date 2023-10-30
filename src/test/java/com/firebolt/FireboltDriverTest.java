package com.firebolt;

import com.firebolt.jdbc.connection.FireboltConnectionServiceSecretAuthentication;
import com.firebolt.jdbc.connection.FireboltConnectionUserPasswordAuthentication;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;

import java.sql.Connection;
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
	void shouldReturnNewConnectionWhenUrlIsValid1() throws SQLException {
		shouldReturnNewConnectionWhenUrlIsValid(FireboltConnectionUserPasswordAuthentication.class, "jdbc:firebolt://api.dev.firebolt.io/db_name");
	}

	@Test
	void shouldReturnNewConnectionWhenUrlIsValid2() throws SQLException {
		shouldReturnNewConnectionWhenUrlIsValid(FireboltConnectionServiceSecretAuthentication.class, "jdbc:firebolt:db_name");
	}

	private <T extends Connection> void shouldReturnNewConnectionWhenUrlIsValid(Class<T> connectionType, String jdbcUrl) throws SQLException {
		try (MockedConstruction<T> mocked = mockConstruction(connectionType)) {
			FireboltDriver fireboltDriver = new FireboltDriver();
			assertNotNull(fireboltDriver.connect(jdbcUrl, new Properties()));
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
					"jdbc:firebolt://api.dev.firebolt.io/db_name,,host=api.dev.firebolt.io;path=/db_name",
					"jdbc:firebolt://api.dev.firebolt.io/db_name?account=test,,host=api.dev.firebolt.io;path=/db_name;account=test",
					"jdbc:firebolt://api.dev.firebolt.io/db_name?account=test,user=usr;password=pwd,host=api.dev.firebolt.io;path=/db_name;account=test;user=usr;password=pwd", // legit:ignore-secrets
					"jdbc:firebolt://api.dev.firebolt.io/db_name,user=usr;password=pwd,host=api.dev.firebolt.io;path=/db_name;user=usr;password=pwd", // legit:ignore-secrets
					// TODO: add more tests with "new" URL format
//					"jdbc:firebolt:db_name,,host=api.dev.firebolt.io;database=db_name",
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
