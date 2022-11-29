package com.firebolt.jdbc.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.firebolt.jdbc.client.account.FireboltAccountClient;
import com.firebolt.jdbc.client.account.response.FireboltAccountResponse;
import com.firebolt.jdbc.client.account.response.FireboltDefaultDatabaseEngineResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineIdResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineResponse;
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
	void shouldGetDefaultDbEngineWhenEngineNameIsNullOrEmpty() throws Exception {
		FireboltProperties properties = FireboltProperties.builder().host(HOST).account(ACCOUNT_ID).database(DB_NAME)
				.compress(false).build();

		when(fireboltAccountClient.getAccount(properties.getHost(), properties.getAccount(), ACCESS_TOKEN))
				.thenReturn(FireboltAccountResponse.builder().accountId(ACCOUNT_ID).build());
		when(fireboltAccountClient.getDbDefaultEngineAddressByDbName(HOST, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN))
				.thenReturn(FireboltDefaultDatabaseEngineResponse.builder().engineUrl("URL").build());
		fireboltEngineService.getEngine(HOST, properties, ACCESS_TOKEN);

		verify(fireboltAccountClient).getAccount(properties.getHost(), properties.getAccount(), ACCESS_TOKEN);
		verify(fireboltAccountClient).getDbDefaultEngineAddressByDbName(HOST, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN);
		verifyNoMoreInteractions(fireboltAccountClient);
	}

	@Test
	void shouldGThrowExceptionWhenGettingDefaultEngineAndTheUrlReturnedFromTheServerIsNull() throws Exception {
		FireboltProperties properties = FireboltProperties.builder().host(HOST).account(ACCOUNT_ID).database(DB_NAME)
				.compress(false).build();

		when(fireboltAccountClient.getAccount(properties.getHost(), properties.getAccount(), ACCESS_TOKEN))
				.thenReturn(FireboltAccountResponse.builder().accountId(ACCOUNT_ID).build());
		when(fireboltAccountClient.getDbDefaultEngineAddressByDbName(HOST, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN))
				.thenReturn(FireboltDefaultDatabaseEngineResponse.builder().engineUrl(null).build());
		FireboltException exception = assertThrows(FireboltException.class,
				() -> fireboltEngineService.getEngine(HOST, properties, ACCESS_TOKEN));
		assertEquals(
				"There is no Firebolt engine running on https://host attached to the database dbName. To connect first make sure there is a running engine and then try again.",
				exception.getMessage());

		verify(fireboltAccountClient).getAccount(properties.getHost(), properties.getAccount(), ACCESS_TOKEN);
		verify(fireboltAccountClient).getDbDefaultEngineAddressByDbName(HOST, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN);
		verifyNoMoreInteractions(fireboltAccountClient);
	}

	@Test
	void shouldGetEngineAddressWhenEngineNameIsPresent() throws Exception {
		FireboltProperties properties = FireboltProperties.builder().host(HOST).account(ACCOUNT_ID).database(DB_NAME)
				.engine(ENGINE_NAME).compress(false).build();
		when(fireboltAccountClient.getAccount(properties.getHost(), properties.getAccount(), ACCESS_TOKEN))
				.thenReturn(FireboltAccountResponse.builder().accountId(ACCOUNT_ID).build());
		when(fireboltAccountClient.getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN))
				.thenReturn(FireboltEngineIdResponse.builder()
						.engine(FireboltEngineIdResponse.Engine.builder().engineId(ENGINE_ID).build()).build());
		when(fireboltAccountClient.getEngine(HOST, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN))
				.thenReturn(FireboltEngineResponse.builder()
						.engine(FireboltEngineResponse.Engine.builder().endpoint("ANY").build()).build());
		fireboltEngineService.getEngine(HOST, properties, ACCESS_TOKEN);

		verify(fireboltAccountClient).getAccount(properties.getHost(), ACCOUNT_ID, ACCESS_TOKEN);
		verify(fireboltAccountClient).getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
		verify(fireboltAccountClient).getEngine(HOST, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN);
		verifyNoMoreInteractions(fireboltAccountClient);
	}

	@Test
	void shouldThrowExceptionWhenEngineNameIsSpecifiedButUrlIsNotPresentInTheResponse() throws Exception {
		FireboltProperties properties = FireboltProperties.builder().host(HOST).account(ACCOUNT_ID).database(DB_NAME)
				.engine(ENGINE_NAME).compress(false).build();
		when(fireboltAccountClient.getAccount(properties.getHost(), properties.getAccount(), ACCESS_TOKEN))
				.thenReturn(FireboltAccountResponse.builder().accountId(ACCOUNT_ID).build());
		when(fireboltAccountClient.getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN))
				.thenReturn(FireboltEngineIdResponse.builder()
						.engine(FireboltEngineIdResponse.Engine.builder().engineId(ENGINE_ID).build()).build());
		when(fireboltAccountClient.getEngine(HOST, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN))
				.thenReturn(FireboltEngineResponse.builder()
						.engine(FireboltEngineResponse.Engine.builder().endpoint(null).build()).build());
		FireboltException exception = assertThrows(FireboltException.class,
				() -> fireboltEngineService.getEngine(HOST, properties, ACCESS_TOKEN));
		assertEquals(
				"There is no Firebolt engine running on https://host with the name engineName. To connect first make sure there is a running engine and then try again.",
				exception.getMessage());

		verify(fireboltAccountClient).getAccount(properties.getHost(), ACCOUNT_ID, ACCESS_TOKEN);
		verify(fireboltAccountClient).getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
		verify(fireboltAccountClient).getEngine(HOST, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN);
		verifyNoMoreInteractions(fireboltAccountClient);
	}

	@Test
	void shouldThrowExceptionWhenEngineNameIsSpecifiedButEngineIdIsNotPresentInTheServerResponse() throws Exception {
		FireboltProperties properties = FireboltProperties.builder().host(HOST).account(ACCOUNT_ID).database(DB_NAME)
				.engine(ENGINE_NAME).compress(false).build();
		when(fireboltAccountClient.getAccount(properties.getHost(), properties.getAccount(), ACCESS_TOKEN))
				.thenReturn(FireboltAccountResponse.builder().accountId(ACCOUNT_ID).build());
		when(fireboltAccountClient.getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN))
				.thenReturn(FireboltEngineIdResponse.builder()
						.engine(FireboltEngineIdResponse.Engine.builder().engineId(null).build()).build());
		FireboltException exception = assertThrows(FireboltException.class,
				() -> fireboltEngineService.getEngine(HOST, properties, ACCESS_TOKEN));
		assertEquals(
				"Failed to extract engine id field from the server response: the response from the server is invalid.",
				exception.getMessage());
		verify(fireboltAccountClient).getAccount(properties.getHost(), ACCOUNT_ID, ACCESS_TOKEN);
		verify(fireboltAccountClient).getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
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

	@ParameterizedTest
	@ValueSource(strings = { "ENGINE_STATUS_PROVISIONING_STARTED", "ENGINE_STATUS_PROVISIONING_FINISHED",
			"ENGINE_STATUS_PROVISIONING_PENDING" })
	void shouldThrowExceptionWhenEngineStatusIndicatesEngineIsStarting(String status) throws Exception {
		FireboltProperties properties = FireboltProperties.builder().host(HOST).account(ACCOUNT_ID).database(DB_NAME)
				.engine(ENGINE_NAME).compress(false).build();
		when(fireboltAccountClient.getAccount(properties.getHost(), properties.getAccount(), ACCESS_TOKEN))
				.thenReturn(FireboltAccountResponse.builder().accountId(ACCOUNT_ID).build());
		when(fireboltAccountClient.getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN))
				.thenReturn(FireboltEngineIdResponse.builder()
						.engine(FireboltEngineIdResponse.Engine.builder().engineId(ENGINE_ID).build()).build());
		when(fireboltAccountClient.getEngine(HOST, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN))
				.thenReturn(FireboltEngineResponse.builder()
						.engine(FireboltEngineResponse.Engine.builder().endpoint("ANY").currentStatus(status).build())
						.build());
		FireboltException exception = assertThrows(FireboltException.class,
				() -> fireboltEngineService.getEngine(HOST, properties, ACCESS_TOKEN));
		assertEquals("The engine engineName is currently starting and should be ready in a few minutes.",
				exception.getMessage());

		verify(fireboltAccountClient).getAccount(properties.getHost(), ACCOUNT_ID, ACCESS_TOKEN);
		verify(fireboltAccountClient).getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
		verify(fireboltAccountClient).getEngine(HOST, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN);
		verifyNoMoreInteractions(fireboltAccountClient);
	}
}
