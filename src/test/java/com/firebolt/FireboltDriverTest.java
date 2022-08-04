package com.firebolt;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockConstruction;

import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import com.firebolt.jdbc.connection.FireboltConnection;

class FireboltDriverTest {

	@Test
	void shouldNotReturnNewConnectionWhenUrlIsInvalid() throws SQLException {
		FireboltDriver fireboltDriver = new FireboltDriver();
		assertNull(fireboltDriver.connect("invalid:url", null));
	}

	@Test
	void shouldReturnNewConnectionWhenUrlIsValid() throws SQLException {
		try (MockedConstruction<FireboltConnection> mocked = mockConstruction(FireboltConnection.class)) {
			FireboltDriver fireboltDriver = new FireboltDriver();
			assertNotNull(fireboltDriver.connect("jdbc:firebolt://api.dev.firebolt.io/db_name", new Properties()));
			assertEquals(1, mocked.constructed().size());
		}
	}
}