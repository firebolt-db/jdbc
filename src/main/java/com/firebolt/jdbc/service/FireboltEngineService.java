package com.firebolt.jdbc.service;

import com.firebolt.jdbc.client.account.FireboltAccountClient;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;

@RequiredArgsConstructor
@Slf4j
public class FireboltEngineService {
  private final FireboltAccountClient fireboltAccountClient;

  public String getEngineHost(String connectionUrl, FireboltProperties loginProperties)
      throws FireboltException {
    String accountId = null;
    try {
      if (StringUtils.isNotEmpty(loginProperties.getAccount())) {
        accountId =
            fireboltAccountClient
                .getAccountId(connectionUrl, loginProperties.getAccount())
                .orElse(null);
      }
      if (StringUtils.isEmpty(loginProperties.getEngine()))
        return fireboltAccountClient.getDbDefaultEngineAddress(
            connectionUrl, accountId, loginProperties.getDatabase());
      String engineID =
          fireboltAccountClient.getEngineId(connectionUrl, accountId, loginProperties.getEngine());
      return fireboltAccountClient.getEngineAddress(
          connectionUrl, accountId, loginProperties.getEngine(), engineID);
    } catch (FireboltException e) {
      throw e;
    } catch (Exception e) {
      throw new FireboltException(
          String.format("Could not get engine host at %s", connectionUrl), e);
    }
  }

  public String getEngineNameFromHost(String engineHost) throws FireboltException {
    return Optional.ofNullable(engineHost)
        .filter(host -> host.contains("."))
        .map(host -> host.split("\\.")[0])
        .map(host -> host.replace("-", "_"))
        .orElseThrow(
            () ->
                new FireboltException(
                    String.format("Could not establish the engine from the host: %s", engineHost)));
  }
}
