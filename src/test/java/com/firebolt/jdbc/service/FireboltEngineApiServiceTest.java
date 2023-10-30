package com.firebolt.jdbc.service;

import com.firebolt.jdbc.client.account.FireboltAccountClient;
import com.firebolt.jdbc.client.account.response.FireboltAccountResponse;
import com.firebolt.jdbc.client.account.response.FireboltDefaultDatabaseEngineResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineIdResponse;
import com.firebolt.jdbc.client.account.response.FireboltEngineResponse;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FireboltEngineApiServiceTest {
    private static final String HOST = "host";
    private static final String URL = "http://host";
    private static final String ACCOUNT_ID = "account_id";
    private static final String DB_NAME = "dbName";
    private static final String ENGINE_NAME = "engineName";
    private static final String ENGINE_ID = "engineId";
    private static final String ACCESS_TOKEN = "token";

    @Mock
    private FireboltAccountClient fireboltAccountClient;

    @InjectMocks
    private FireboltEngineApiService fireboltEngineService;

    @Test
    void shouldGetDefaultDbEngineWhenEngineNameIsNullOrEmpty() throws Exception {
        FireboltProperties properties = FireboltProperties.builder().host(HOST).account(ACCOUNT_ID).database(DB_NAME)
                .compress(false).accessToken(ACCESS_TOKEN).build();

        when(fireboltAccountClient.getAccount(properties.getHttpConnectionUrl(), properties.getAccount(), ACCESS_TOKEN))
                .thenReturn(FireboltAccountResponse.builder().accountId(ACCOUNT_ID).build());
        when(fireboltAccountClient.getDefaultEngineByDatabaseName(URL, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN))
                .thenReturn(FireboltDefaultDatabaseEngineResponse.builder().engineUrl("URL").build());
        fireboltEngineService.getEngine(properties);

        verify(fireboltAccountClient).getAccount(URL, properties.getAccount(), ACCESS_TOKEN);
        verify(fireboltAccountClient).getDefaultEngineByDatabaseName(URL, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN);
        verifyNoMoreInteractions(fireboltAccountClient);
    }

    @Test
    void shouldGThrowExceptionWhenGettingDefaultEngineAndTheUrlReturnedFromTheServerIsNull() throws Exception {
        FireboltProperties properties = FireboltProperties.builder().host(HOST).account(ACCOUNT_ID).database(DB_NAME)
                .compress(false).accessToken(ACCESS_TOKEN).build();

        when(fireboltAccountClient.getAccount(properties.getHttpConnectionUrl(), properties.getAccount(), ACCESS_TOKEN))
                .thenReturn(FireboltAccountResponse.builder().accountId(ACCOUNT_ID).build());
        when(fireboltAccountClient.getDefaultEngineByDatabaseName(URL, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN))
                .thenReturn(FireboltDefaultDatabaseEngineResponse.builder().engineUrl(null).build());
        FireboltException exception = assertThrows(FireboltException.class,
                () -> fireboltEngineService.getEngine(properties));
        assertEquals(
                "There is no Firebolt engine running on http://host attached to the database dbName. To connect first make sure there is a running engine and then try again.",
                exception.getMessage());

        verify(fireboltAccountClient).getAccount(properties.getHttpConnectionUrl(), properties.getAccount(), ACCESS_TOKEN);
        verify(fireboltAccountClient).getDefaultEngineByDatabaseName(URL, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN);
        verifyNoMoreInteractions(fireboltAccountClient);
    }

    @Test
    void shouldGetEngineWhenEngineNameIsPresent() throws Exception {
        FireboltProperties properties = FireboltProperties.builder().host(HOST).account(ACCOUNT_ID).database(DB_NAME)
                .engine(ENGINE_NAME).compress(false).accessToken(ACCESS_TOKEN).build();
        when(fireboltAccountClient.getAccount(URL, properties.getAccount(), ACCESS_TOKEN))
                .thenReturn(FireboltAccountResponse.builder().accountId(ACCOUNT_ID).build());
        when(fireboltAccountClient.getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN))
                .thenReturn(FireboltEngineIdResponse.builder()
                        .engine(FireboltEngineIdResponse.Engine.builder().engineId(ENGINE_ID).build()).build());
        when(fireboltAccountClient.getEngine(URL, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN))
                .thenReturn(FireboltEngineResponse.builder()
                        .engine(FireboltEngineResponse.Engine.builder().endpoint("ANY").build()).build());
        fireboltEngineService.getEngine(properties);

        verify(fireboltAccountClient).getAccount(URL, ACCOUNT_ID, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngine(URL, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN);
        verifyNoMoreInteractions(fireboltAccountClient);
    }

    @Test
    void shouldNotGetAccountWhileGettingEngineIfAccountIdIsNotPresent() throws Exception {
        FireboltProperties properties = FireboltProperties.builder().host(HOST).database(DB_NAME)
                .engine(ENGINE_NAME).compress(false).accessToken(ACCESS_TOKEN).build();
        when(fireboltAccountClient.getEngineId(URL, null, ENGINE_NAME, ACCESS_TOKEN))
                .thenReturn(FireboltEngineIdResponse.builder()
                        .engine(FireboltEngineIdResponse.Engine.builder().engineId(ENGINE_ID).build()).build());
        when(fireboltAccountClient.getEngine(URL, null, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN))
                .thenReturn(FireboltEngineResponse.builder()
                        .engine(FireboltEngineResponse.Engine.builder().endpoint("ANY").build()).build());
        fireboltEngineService.getEngine(properties);

        verify(fireboltAccountClient, times(0)).getAccount(any(), any(), any());
        verify(fireboltAccountClient).getEngineId(URL, null, ENGINE_NAME, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngine(URL, null, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN);
        verifyNoMoreInteractions(fireboltAccountClient);
    }

    @Test
    void shouldThrowExceptionWhenEngineNameIsSpecifiedButUrlIsNotPresentInTheResponse() throws Exception {
        FireboltProperties properties = FireboltProperties.builder().host(HOST).account(ACCOUNT_ID).database(DB_NAME)
                .engine(ENGINE_NAME).compress(false).accessToken(ACCESS_TOKEN).build();
        when(fireboltAccountClient.getAccount(properties.getHttpConnectionUrl(), properties.getAccount(), ACCESS_TOKEN))
                .thenReturn(FireboltAccountResponse.builder().accountId(ACCOUNT_ID).build());
        when(fireboltAccountClient.getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN))
                .thenReturn(FireboltEngineIdResponse.builder()
                        .engine(FireboltEngineIdResponse.Engine.builder().engineId(ENGINE_ID).build()).build());
        when(fireboltAccountClient.getEngine(URL, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN))
                .thenReturn(FireboltEngineResponse.builder()
                        .engine(FireboltEngineResponse.Engine.builder().endpoint(null).build()).build());
        FireboltException exception = assertThrows(FireboltException.class,
                () -> fireboltEngineService.getEngine(properties));
        assertEquals(
                "There is no Firebolt engine running on http://host with the name engineName. To connect first make sure there is a running engine and then try again.",
                exception.getMessage());

        verify(fireboltAccountClient).getAccount(properties.getHttpConnectionUrl(), ACCOUNT_ID, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngine(URL, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN);
        verifyNoMoreInteractions(fireboltAccountClient);
    }

    @Test
    void shouldThrowExceptionWhenEngineNameIsSpecifiedButEngineIdIsNotPresentInTheServerResponse() throws Exception {
        FireboltProperties properties = FireboltProperties.builder().host(HOST).account(ACCOUNT_ID).database(DB_NAME)
                .engine(ENGINE_NAME).compress(false).accessToken(ACCESS_TOKEN).build();
        when(fireboltAccountClient.getAccount(properties.getHttpConnectionUrl(), properties.getAccount(), ACCESS_TOKEN))
                .thenReturn(FireboltAccountResponse.builder().accountId(ACCOUNT_ID).build());
        when(fireboltAccountClient.getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN))
                .thenReturn(FireboltEngineIdResponse.builder()
                        .engine(FireboltEngineIdResponse.Engine.builder().engineId(null).build()).build());
        FireboltException exception = assertThrows(FireboltException.class,
                () -> fireboltEngineService.getEngine(properties));
        assertEquals(
                "Failed to extract engine id field from the server response: the response from the server is invalid.",
                exception.getMessage());
        verify(fireboltAccountClient).getAccount(properties.getHttpConnectionUrl(), ACCOUNT_ID, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
        verifyNoMoreInteractions(fireboltAccountClient);
    }

    @ParameterizedTest
    @ValueSource(strings = { "ENGINE_STATUS_PROVISIONING_STARTED", "ENGINE_STATUS_PROVISIONING_FINISHED",
            "ENGINE_STATUS_PROVISIONING_PENDING" })
    void shouldThrowExceptionWhenEngineStatusIndicatesEngineIsStarting(String status) throws Exception {
        FireboltProperties properties = FireboltProperties.builder().host(HOST).account(ACCOUNT_ID).database(DB_NAME)
                .engine(ENGINE_NAME).compress(false).accessToken(ACCESS_TOKEN).build();
        when(fireboltAccountClient.getAccount(properties.getHttpConnectionUrl(), properties.getAccount(), ACCESS_TOKEN))
                .thenReturn(FireboltAccountResponse.builder().accountId(ACCOUNT_ID).build());
        when(fireboltAccountClient.getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN))
                .thenReturn(FireboltEngineIdResponse.builder()
                        .engine(FireboltEngineIdResponse.Engine.builder().engineId(ENGINE_ID).build()).build());
        when(fireboltAccountClient.getEngine(URL, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN))
                .thenReturn(FireboltEngineResponse.builder()
                        .engine(FireboltEngineResponse.Engine.builder().endpoint("ANY").currentStatus(status).build())
                        .build());
        FireboltException exception = assertThrows(FireboltException.class,
                () -> fireboltEngineService.getEngine(properties));
        assertEquals("The engine engineName is currently starting. Please wait until the engine is on and then execute the query again.",
                exception.getMessage());

        verify(fireboltAccountClient).getAccount(properties.getHttpConnectionUrl(), ACCOUNT_ID, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngine(URL, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN);
        verifyNoMoreInteractions(fireboltAccountClient);
    }
}
