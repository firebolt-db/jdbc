package io.firebolt.jdbc.service;

import io.firebolt.jdbc.client.account.FireboltAccountClient;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltEngineServiceTest {

  private static final String HOST = "https://host";
  private static final String ACCOUNT_ID = "account_id";
  private static final String DB_NAME = "dbName";
  private static final String ENGINE_NAME = "engineName";
  private static final String ENGINE_ID = "engineId";
  private static final Boolean IS_COMPRESS = false;

  @Mock private FireboltAccountClient fireboltAccountClient;

  @InjectMocks private FireboltEngineService fireboltEngineService;

  @Test
  void shouldGetDbAddressWhenEngineNameIsNullOrEmpty() throws Exception {
    FireboltProperties properties =
        FireboltProperties.builder()
            .host(HOST)
            .account(ACCOUNT_ID)
            .database(DB_NAME)
            .compress(0)
            .build();

    when(fireboltAccountClient.getAccountId(properties.getHost(), properties.getAccount()))
        .thenReturn(Optional.of(ACCOUNT_ID));

    fireboltEngineService.getEngineHost(HOST, properties);

    verify(fireboltAccountClient).getAccountId(properties.getHost(), properties.getAccount());
    verify(fireboltAccountClient).getDbDefaultEngineAddress(HOST, ACCOUNT_ID, DB_NAME);
    verifyNoMoreInteractions(fireboltAccountClient);
  }

  @Test
  void shouldGetEngineAddressWhenEngineNameIsPresent() throws Exception {
    FireboltProperties properties =
        FireboltProperties.builder()
            .host(HOST)
            .account(ACCOUNT_ID)
            .database(DB_NAME)
            .engine(ENGINE_NAME)
            .compress(0)
            .build();
    when(fireboltAccountClient.getAccountId(properties.getHost(), properties.getAccount()))
        .thenReturn(Optional.of(ACCOUNT_ID));
    when(fireboltAccountClient.getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME)).thenReturn(ENGINE_ID);

    fireboltEngineService.getEngineHost(HOST, properties);

    verify(fireboltAccountClient).getAccountId(properties.getHost(), ACCOUNT_ID);
    verify(fireboltAccountClient).getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME);
    verify(fireboltAccountClient).getEngineAddress(HOST, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID);
    verifyNoMoreInteractions(fireboltAccountClient);
  }
}
