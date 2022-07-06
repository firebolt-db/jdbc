package io.firebolt.jdbc.service;

import io.firebolt.jdbc.client.account.FireboltAccountClient;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@RequiredArgsConstructor
public class FireboltEngineService {
  private final FireboltAccountClient fireboltAccountClient;

  public String getEngineHost(String host, FireboltProperties loginProperties)
      throws FireboltException {
    String accountId = null;
    try {
      if (StringUtils.isNotEmpty(loginProperties.getAccount())) {
        accountId =
            fireboltAccountClient
                .getAccountId(host, loginProperties.getAccount())
                .orElse(null);
      }
      if (StringUtils.isEmpty(loginProperties.getEngine()))
        return fireboltAccountClient.getDbDefaultEngineAddress(
            host, accountId, loginProperties.getDatabase());
      String engineID =
          fireboltAccountClient.getEngineId(
              host, accountId, loginProperties.getEngine());
      return fireboltAccountClient.getEngineAddress(
          host, accountId, loginProperties.getEngine(), engineID);
    } catch (Exception e) {
      throw new FireboltException(String.format("Could not get engine host at %s", host), e);
    }
  }
}
