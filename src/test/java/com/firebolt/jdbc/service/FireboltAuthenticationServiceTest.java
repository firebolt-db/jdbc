package com.firebolt.jdbc.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.connection.FireboltConnectionTokens;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;

@ExtendWith(MockitoExtension.class)
class FireboltAuthenticationServiceTest {

	private static final String USER = "usr";
	private static final String PASSWORD = "PA§§WORD";

	private static final String ENV = "ENV";

	private static final FireboltProperties PROPERTIES = FireboltProperties.builder().principal(USER).secret(PASSWORD).environment(ENV).compress(true).build();

	@Mock
	private FireboltAuthenticationClient fireboltAuthenticationClient;

	private FireboltAuthenticationService fireboltAuthenticationService;

	@BeforeEach
	void setUp() {
		fireboltAuthenticationService = new FireboltAuthenticationService(fireboltAuthenticationClient);
	}

	@Test
	void shouldGetConnectionToken() throws SQLException, IOException {
		String randomHost = UUID.randomUUID().toString();
		FireboltConnectionTokens tokens = new FireboltConnectionTokens("access", 52);
		when(fireboltAuthenticationClient.postConnectionTokens(randomHost, USER, PASSWORD, ENV)).thenReturn(tokens);

		assertEquals(tokens, fireboltAuthenticationService.getConnectionTokens(randomHost, PROPERTIES));
		verify(fireboltAuthenticationClient).postConnectionTokens(randomHost, USER, PASSWORD, ENV);
	}

	@Test
	void shouldCallClientOnlyOnceWhenServiceCalledTwiceForTheSameHost() throws SQLException, IOException {
		String randomHost = UUID.randomUUID().toString();
		FireboltConnectionTokens tokens = new FireboltConnectionTokens("access", 52);
		when(fireboltAuthenticationClient.postConnectionTokens(randomHost, USER, PASSWORD, ENV)).thenReturn(tokens);

		fireboltAuthenticationService.getConnectionTokens(randomHost, PROPERTIES);
		assertEquals(tokens, fireboltAuthenticationService.getConnectionTokens(randomHost, PROPERTIES));
		verify(fireboltAuthenticationClient).postConnectionTokens(randomHost, USER, PASSWORD, ENV);
	}

	@Test
	void shouldGetConnectionTokenAfterRemoving() throws SQLException, IOException {
		String randomHost = UUID.randomUUID().toString();
		FireboltConnectionTokens token1 = new FireboltConnectionTokens("one", 52);
		FireboltConnectionTokens token2 = new FireboltConnectionTokens("two", 52);
		when(fireboltAuthenticationClient.postConnectionTokens(randomHost, USER, PASSWORD, ENV)).thenReturn(token1, token2);

		fireboltAuthenticationService.getConnectionTokens(randomHost, PROPERTIES);
		assertEquals(token1, fireboltAuthenticationService.getConnectionTokens(randomHost, PROPERTIES));
		fireboltAuthenticationService.removeConnectionTokens(randomHost, PROPERTIES);
		fireboltAuthenticationService.getConnectionTokens(randomHost, PROPERTIES);
		assertEquals(token2, fireboltAuthenticationService.getConnectionTokens(randomHost, PROPERTIES));

		verify(fireboltAuthenticationClient, Mockito.times(2)).postConnectionTokens(randomHost, USER, PASSWORD, ENV);
	}

	@Test
	void shouldThrowExceptionWithServerResponseWhenAResponseIsAvailable() throws SQLException, IOException {
		String randomHost = UUID.randomUUID().toString();
		Mockito.when(fireboltAuthenticationClient.postConnectionTokens(randomHost, USER, PASSWORD, ENV))
				.thenThrow(new FireboltException("An error happened during authentication", 403, "INVALID PASSWORD"));

		FireboltException ex = assertThrows(FireboltException.class,
				() -> fireboltAuthenticationService.getConnectionTokens(randomHost, PROPERTIES));
		assertEquals(
				"Failed to connect to Firebolt with the error from the server: INVALID PASSWORD, see logs for more info.",
				ex.getMessage());
	}

	@Test
	void shouldThrowExceptionWithExceptionMessageWhenAResponseIsNotAvailable() throws SQLException, IOException {
		String randomHost = UUID.randomUUID().toString();
		Mockito.when(fireboltAuthenticationClient.postConnectionTokens(randomHost, USER, PASSWORD, ENV))
				.thenThrow(new NullPointerException("NULL!"));

		FireboltException ex = assertThrows(FireboltException.class,
				() -> fireboltAuthenticationService.getConnectionTokens(randomHost, PROPERTIES));
		assertEquals("Failed to connect to Firebolt with the error: NULL!, see logs for more info.", ex.getMessage());
	}

}
