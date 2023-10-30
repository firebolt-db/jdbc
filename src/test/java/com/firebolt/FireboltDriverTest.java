package com.firebolt;

import com.firebolt.jdbc.connection.FireboltConnectionUserPassword;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;

import java.io.IOException;
import java.io.StringReader;
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

	@ParameterizedTest
	@CsvSource({
			"FireboltConnectionServiceSecret, jdbc:firebolt://api.dev.firebolt.io/db_name,", // old URL, null properties, i.e. no username - for "URL backwards compatibility" considered v2
			"FireboltConnectionServiceSecret, jdbc:firebolt://api.dev.firebolt.io/db_name,''", // same but empty properties
			"FireboltConnectionServiceSecret,jdbc:firebolt:db_name,", // new URL format, null properties
			"FireboltConnectionServiceSecret,jdbc:firebolt:db_name,''", // same but empty properties
			"FireboltConnectionUserPassword, jdbc:firebolt://api.dev.firebolt.io/db_name,'user=sherlok@holmes.uk;password=watson'", // user is email - v1
			"FireboltConnectionServiceSecret, jdbc:firebolt://api.dev.firebolt.io/db_name,'user=not-email;password=any'", // user is not email - v2
			"FireboltConnectionServiceSecret, jdbc:firebolt://api.dev.firebolt.io/db_name,'client_id=not-email;client_secret=any'", // clientId and client_secret are defined - v2
			"FireboltConnectionUserPassword, jdbc:firebolt://api.dev.firebolt.io/db_name?user=sherlok@holmes.uk&password=watson,", // user is email as URL parameter - v1 // legit:ignore-secrets
			"FireboltConnectionServiceSecret, jdbc:firebolt://api.dev.firebolt.io/db_name?client_id=not-email&client_secret=any,", // clientId and client_secret as URL parameters - v2
	})
	void validateConnectionWhenUrlIsValid(String expectedConnectionTypeName, String jdbcUrl, String propsString) throws SQLException, IOException, ClassNotFoundException {
		Properties properties = null;
		if (propsString != null) {
			properties = new Properties();
			properties.load(new StringReader(propsString));
		}
		@SuppressWarnings("unchecked")
		Class<? extends Connection> expectedConnectionType = (Class<? extends Connection>)Class.forName(FireboltConnectionUserPassword.class.getPackageName() + "." + expectedConnectionTypeName);
		validateConnection(expectedConnectionType, jdbcUrl, properties);
	}

	private <T extends Connection> void validateConnection(Class<T> expectedConnectionType, String jdbcUrl, Properties properties) throws SQLException {
		try (MockedConstruction<T> mocked = mockConstruction(expectedConnectionType)) {
			FireboltDriver fireboltDriver = new FireboltDriver();
			assertNotNull(fireboltDriver.connect(jdbcUrl, properties));
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
