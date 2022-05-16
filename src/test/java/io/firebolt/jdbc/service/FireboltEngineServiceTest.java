package io.firebolt.jdbc.service;

import io.firebolt.jdbc.client.account.FireboltAccountClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class FireboltEngineServiceTest {

  private static final String ACCESS_TOKEN = "token";
  private static final String HOST = "https://host";
  private static final String ACCOUNT = "account";
  private static final String ACCOUNT_ID = "account_id";
  private static final String DB_NAME = "dbName";
  private static final String ENGINE_NAME = "engineName";
  private static final String ENGINE_ID = "engineId";

  @Mock private FireboltAccountClient fireboltAccountClient;

  @InjectMocks private FireboltEngineService fireboltEngineService;

  @Test
  void shouldGetDbAddressWhenEngineNameIsNullOrEmpty() throws Exception {
    when(fireboltAccountClient.getAccountId(HOST, ACCOUNT, ACCESS_TOKEN))
        .thenReturn(Optional.of(ACCOUNT_ID));

    fireboltEngineService.getEngineAddress(HOST, DB_NAME, null, ACCOUNT, ACCESS_TOKEN);

    verify(fireboltAccountClient).getAccountId(HOST, ACCOUNT, ACCESS_TOKEN);
    verify(fireboltAccountClient)
        .getDbDefaultEngineAddress(HOST, ACCOUNT_ID, DB_NAME, ACCESS_TOKEN);
    verifyNoMoreInteractions(fireboltAccountClient);
  }

  @Test
  void shouldGetEngineAddressWhenEngineNameIsNotNullOrEmpty() throws Exception {
    when(fireboltAccountClient.getAccountId(HOST, ACCOUNT, ACCESS_TOKEN))
        .thenReturn(Optional.of(ACCOUNT_ID));
    when(fireboltAccountClient.getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN))
        .thenReturn(ENGINE_ID);

    fireboltEngineService.getEngineAddress(HOST, DB_NAME, ENGINE_NAME, ACCOUNT, ACCESS_TOKEN);

    verify(fireboltAccountClient).getAccountId(HOST, ACCOUNT, ACCESS_TOKEN);
    verify(fireboltAccountClient).getEngineId(HOST, ACCOUNT_ID, ENGINE_NAME, ACCESS_TOKEN);
    verify(fireboltAccountClient)
        .getEngineAddress(HOST, ACCOUNT_ID, ENGINE_NAME, ENGINE_ID, ACCESS_TOKEN);
    verifyNoMoreInteractions(fireboltAccountClient);
  }
}
