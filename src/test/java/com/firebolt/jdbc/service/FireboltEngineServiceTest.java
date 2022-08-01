package com.firebolt.jdbc.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import java.sql.SQLException;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.firebolt.jdbc.client.account.FireboltAccountClient;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;

@ExtendWith(MockitoExtension.class)
class FireboltEngineServiceTest {

	private static final String HOST = "https://host";
	private static final String ACCOUNT_ID = "account_id";
	private static final String DB_NAME = "dbName";
	private static final String ENGINE_NAME = "engineName";
	private static final String ENGINE_ID = "engineId";
	private static final String ACCESS_TOKEN = "token";

	@Mock
	private FireboltAccountClient fireboltAccountClient;

	@InjectMocks
	private FireboltEngineService fireboltEngineService;

	@Test
	void shouldGetDbAddressWhenEngineNameIsNullOrEmpty() throws Exception {
		FireboltProperties properties = FireboltProperties.builder().host(HOST).account(ACCOUNT_ID).database(DB_NAME)
				.compress(false).build();

		when(fireboltAccountClient.getAccountId(properties.getHost(), properties.getAccount(), ACCESS_TOKEN))
				.thenReturn(Optional.of(ACCOUNT_ID));

		fireboltEngineService.getEngineHost(HOST, properties, ACCESS_TOKEN);

		verify(fireboltAccountClient).getAccountId(properties.getHost(), properties.getAccount(), ACCESS_TOKEN);
		verify(fireboltAccountClient).getDbDefaultEngineAddress(HOST, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN);
		verifyNoMoreInteractions(fireboltAccountClient);
	}

	@Test
	void shouldGetEngineAddressWhenEngineNameIsPresent() throws Exception {
		FireboltProperties properties = FireboltProperties.builder().host(HOST).account(ACCOUNT_ID).database(DB_NAME)
				.engine(ENGINE_NAME).compress(false).build();
		when(fireboltAccountClient.getAccountId(properties.getHost(), properties.getAccount(), ACCESS_TOKEN))
				.thenReturn(Optional.of(ACCOUNT_ID));
		when(fireboltAccountClient.getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN)).thenReturn(ENGINE_ID);

		fireboltEngineService.getEngineHost(HOST, properties, ACCESS_TOKEN);

		verify(fireboltAccountClient).getAccountId(properties.getHost(), ACCOUNT_ID, ACCESS_TOKEN);
		verify(fireboltAccountClient).getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
		verify(fireboltAccountClient).getEngineAddress(HOST, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN);
		verifyNoMoreInteractions(fireboltAccountClient);
	}

	@Test
	void shouldGetEngineNameFromEngineHost() throws SQLException {
		assertEquals("myHost_345", fireboltEngineService.getEngineNameFromHost("myHost-345.firebolt.io"));
	}

	@Test
	void shouldThrowExceptionWhenThEngineCannotBeEstablishedFromTheHost() {
		assertThrows(FireboltException.class, () -> fireboltEngineService.getEngineNameFromHost("myHost-345"));
	}

	@Test
	void shouldThrowExceptionWhenThEngineCannotBeEstablishedFromNullHost() {
		assertThrows(FireboltException.class, () -> fireboltEngineService.getEngineNameFromHost(null));
	}
}
