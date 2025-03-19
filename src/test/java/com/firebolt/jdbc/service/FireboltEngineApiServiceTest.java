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

import java.io.IOException;

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
    private static final FireboltProperties PROPERTIES_WITHOUT_ENGINE = fireboltProperties(ACCOUNT_ID, null);
    private static final FireboltProperties PROPERTIES_WITHOUT_ACCOUNT = fireboltProperties(null, ENGINE_NAME);
    private static final FireboltProperties PROPERTIES_WITH_ACCOUNT_AND_ENGINE = fireboltProperties(ACCOUNT_ID, ENGINE_NAME);

    @Mock
    private FireboltAccountClient fireboltAccountClient;

    @InjectMocks
    private FireboltEngineApiService fireboltEngineService;

    @Test
    void shouldGetDefaultDbEngineWhenEngineNameIsNullOrEmpty() throws Exception {
        when(fireboltAccountClient.getAccount(PROPERTIES_WITHOUT_ENGINE.getHttpConnectionUrl(), PROPERTIES_WITHOUT_ENGINE.getAccount(), ACCESS_TOKEN))
                .thenReturn(new FireboltAccountResponse(ACCOUNT_ID));
        when(fireboltAccountClient.getDefaultEngineByDatabaseName(URL, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN))
                .thenReturn(new FireboltDefaultDatabaseEngineResponse("URL"));
        fireboltEngineService.getEngine(PROPERTIES_WITHOUT_ENGINE);

        verify(fireboltAccountClient).getAccount(URL, PROPERTIES_WITHOUT_ENGINE.getAccount(), ACCESS_TOKEN);
        verify(fireboltAccountClient).getDefaultEngineByDatabaseName(URL, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN);
        verifyNoMoreInteractions(fireboltAccountClient);
    }

//    @Test
    void shouldThrowExceptionWhenAccountThrowsException() throws Exception {
        when(fireboltAccountClient.getAccount(PROPERTIES_WITHOUT_ENGINE.getHttpConnectionUrl(), PROPERTIES_WITHOUT_ENGINE.getAccount(), ACCESS_TOKEN)).thenThrow(new IOException());
        assertEquals(IOException.class, assertThrows(FireboltException.class, () ->fireboltEngineService.getEngine(PROPERTIES_WITHOUT_ENGINE)).getCause().getClass());
    }

    @Test
    void shouldGThrowExceptionWhenGettingDefaultEngineAndTheUrlReturnedFromTheServerIsNull() throws Exception {
        when(fireboltAccountClient.getAccount(PROPERTIES_WITHOUT_ENGINE.getHttpConnectionUrl(), PROPERTIES_WITHOUT_ENGINE.getAccount(), ACCESS_TOKEN))
                .thenReturn(new FireboltAccountResponse(ACCOUNT_ID));
        when(fireboltAccountClient.getDefaultEngineByDatabaseName(URL, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN))
                .thenReturn(new FireboltDefaultDatabaseEngineResponse(null));
        FireboltException exception = assertThrows(FireboltException.class,
                () -> fireboltEngineService.getEngine(PROPERTIES_WITHOUT_ENGINE));
        assertEquals(
                "There is no Firebolt engine running on http://host attached to the database dbName. To connect first make sure there is a running engine and then try again.",
                exception.getMessage());

        verify(fireboltAccountClient).getAccount(PROPERTIES_WITHOUT_ENGINE.getHttpConnectionUrl(), PROPERTIES_WITHOUT_ENGINE.getAccount(), ACCESS_TOKEN);
        verify(fireboltAccountClient).getDefaultEngineByDatabaseName(URL, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN);
        verifyNoMoreInteractions(fireboltAccountClient);
    }

    @Test
    void shouldGetEngineWhenEngineNameIsPresent() throws Exception {
        when(fireboltAccountClient.getAccount(URL, PROPERTIES_WITH_ACCOUNT_AND_ENGINE.getAccount(), ACCESS_TOKEN))
                .thenReturn(new FireboltAccountResponse(ACCOUNT_ID));
        when(fireboltAccountClient.getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN))
                .thenReturn(new FireboltEngineIdResponse(new FireboltEngineIdResponse.Engine(ENGINE_ID)));
        when(fireboltAccountClient.getEngine(URL, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN))
                .thenReturn(new FireboltEngineResponse(new FireboltEngineResponse.Engine("ANY", null)));
        fireboltEngineService.getEngine(PROPERTIES_WITH_ACCOUNT_AND_ENGINE);

        verify(fireboltAccountClient).getAccount(URL, ACCOUNT_ID, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngine(URL, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN);
        verifyNoMoreInteractions(fireboltAccountClient);
    }

    @Test
    void shouldNotGetAccountWhileGettingEngineIfAccountIdIsNotPresent() throws Exception {
        when(fireboltAccountClient.getEngineId(URL, null, ENGINE_NAME, ACCESS_TOKEN))
                .thenReturn(new FireboltEngineIdResponse(new FireboltEngineIdResponse.Engine(ENGINE_ID)));
        when(fireboltAccountClient.getEngine(URL, null, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN))
                .thenReturn(new FireboltEngineResponse(new FireboltEngineResponse.Engine("ANY", null)));
        fireboltEngineService.getEngine(PROPERTIES_WITHOUT_ACCOUNT);

        verify(fireboltAccountClient, times(0)).getAccount(any(), any(), any());
        verify(fireboltAccountClient).getEngineId(URL, null, ENGINE_NAME, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngine(URL, null, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN);
        verifyNoMoreInteractions(fireboltAccountClient);
    }

    @Test
    void shouldThrowExceptionWhenEngineNameIsSpecifiedButUrlIsNotPresentInTheResponse() throws Exception {
        when(fireboltAccountClient.getAccount(PROPERTIES_WITH_ACCOUNT_AND_ENGINE.getHttpConnectionUrl(), PROPERTIES_WITH_ACCOUNT_AND_ENGINE.getAccount(), ACCESS_TOKEN))
                .thenReturn(new FireboltAccountResponse(ACCOUNT_ID));
        when(fireboltAccountClient.getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN))
                .thenReturn(new FireboltEngineIdResponse(new FireboltEngineIdResponse.Engine(ENGINE_ID)));
        when(fireboltAccountClient.getEngine(URL, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN))
                .thenReturn(new FireboltEngineResponse(new FireboltEngineResponse.Engine(null, null)));
        FireboltException exception = assertThrows(FireboltException.class,
                () -> fireboltEngineService.getEngine(PROPERTIES_WITH_ACCOUNT_AND_ENGINE));
        assertEquals(
                "There is no Firebolt engine running on http://host with the name engineName. To connect first make sure there is a running engine and then try again.",
                exception.getMessage());

        verify(fireboltAccountClient).getAccount(PROPERTIES_WITH_ACCOUNT_AND_ENGINE.getHttpConnectionUrl(), ACCOUNT_ID, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngine(URL, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN);
        verifyNoMoreInteractions(fireboltAccountClient);
    }

    @Test
    void shouldThrowExceptionWhenEngineNameIsSpecifiedButEngineIdIsNotPresentInTheServerResponse() throws Exception {
        when(fireboltAccountClient.getAccount(PROPERTIES_WITH_ACCOUNT_AND_ENGINE.getHttpConnectionUrl(), PROPERTIES_WITH_ACCOUNT_AND_ENGINE.getAccount(), ACCESS_TOKEN))
                .thenReturn(new FireboltAccountResponse(ACCOUNT_ID));
        when(fireboltAccountClient.getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN))
                .thenReturn(new FireboltEngineIdResponse(new FireboltEngineIdResponse.Engine(null)));
        FireboltException exception = assertThrows(FireboltException.class,
                () -> fireboltEngineService.getEngine(PROPERTIES_WITH_ACCOUNT_AND_ENGINE));
        assertEquals(
                "Failed to extract engine id field from the server response: the response from the server is invalid.",
                exception.getMessage());
        verify(fireboltAccountClient).getAccount(PROPERTIES_WITH_ACCOUNT_AND_ENGINE.getHttpConnectionUrl(), ACCOUNT_ID, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
        verifyNoMoreInteractions(fireboltAccountClient);
    }

    @ParameterizedTest
    @ValueSource(strings = { "ENGINE_STATUS_PROVISIONING_STARTED", "ENGINE_STATUS_PROVISIONING_FINISHED", "ENGINE_STATUS_PROVISIONING_PENDING" })
    void shouldThrowExceptionWhenEngineStatusIndicatesEngineIsStarting(String status) throws Exception {
        when(fireboltAccountClient.getAccount(PROPERTIES_WITH_ACCOUNT_AND_ENGINE.getHttpConnectionUrl(), PROPERTIES_WITH_ACCOUNT_AND_ENGINE.getAccount(), ACCESS_TOKEN))
                .thenReturn(new FireboltAccountResponse(ACCOUNT_ID));
        when(fireboltAccountClient.getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN))
                .thenReturn(new FireboltEngineIdResponse(new FireboltEngineIdResponse.Engine(ENGINE_ID)));
        when(fireboltAccountClient.getEngine(URL, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN))
                .thenReturn(new FireboltEngineResponse(new FireboltEngineResponse.Engine("ANY", status)));
        FireboltException exception = assertThrows(FireboltException.class,
                () -> fireboltEngineService.getEngine(PROPERTIES_WITH_ACCOUNT_AND_ENGINE));
        assertEquals("The engine engineName is currently starting. Please wait until the engine is on and then execute the query again.",
                exception.getMessage());

        verify(fireboltAccountClient).getAccount(PROPERTIES_WITH_ACCOUNT_AND_ENGINE.getHttpConnectionUrl(), ACCOUNT_ID, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngineId(URL, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
        verify(fireboltAccountClient).getEngine(URL, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN);
        verifyNoMoreInteractions(fireboltAccountClient);
    }

    private static FireboltProperties fireboltProperties(String account, String engine) {
        return FireboltProperties.builder().host(HOST).account(account).database(DB_NAME).engine(engine).compress(false).accessToken(ACCESS_TOKEN).build();
    }
}
