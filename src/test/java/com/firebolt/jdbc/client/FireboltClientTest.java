package com.firebolt.jdbc.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;

class FireboltClientTest {

	@Test
	void shouldThrowExceptionWhenResponseCodeIs401() {
		CloseableHttpResponse response = mock(CloseableHttpResponse.class);
		HttpEntity entity = mock(HttpEntity.class);
		when(response.getEntity()).thenReturn(entity);
		when(response.getCode()).thenReturn(HttpStatus.SC_UNAUTHORIZED);

		FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
		when(client.getConnection()).thenReturn(mock(FireboltConnection.class));
		FireboltException exception = assertThrows(FireboltException.class,
				() -> client.validateResponse("host", response, false));
		assertEquals(ExceptionType.EXPIRED_TOKEN, exception.getType());
	}

	@Test
	void shouldThrowExceptionWheResponseCodeIsOtherThan2XX() {
		CloseableHttpResponse response = mock(CloseableHttpResponse.class);
		HttpEntity entity = mock(HttpEntity.class);
		when(response.getEntity()).thenReturn(entity);
		when(response.getCode()).thenReturn(HttpStatus.SC_BAD_GATEWAY);

		FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
		when(client.getConnection()).thenReturn(mock(FireboltConnection.class));
		FireboltException exception = assertThrows(FireboltException.class,
				() -> client.validateResponse("host", response, false));
		assertEquals(ExceptionType.ERROR, exception.getType());
	}

	@Test
	void shouldNotThrowExceptionWhenResponseIs2XX() {
		CloseableHttpResponse response = mock(CloseableHttpResponse.class);
		HttpEntity entity = mock(HttpEntity.class);
		when(response.getEntity()).thenReturn(entity);
		when(response.getCode()).thenReturn(HttpStatus.SC_OK);

		FireboltClient client = Mockito.mock(FireboltClient.class, Mockito.CALLS_REAL_METHODS);
		when(client.getConnection()).thenReturn(mock(FireboltConnection.class));
		assertAll(() -> client.validateResponse("host", response, false));
	}
}
