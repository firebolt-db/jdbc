package io.firebolt.jdbc.service;

import io.firebolt.jdbc.client.account.FireboltAccountClient;
import io.firebolt.jdbc.exception.FireboltException;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@RequiredArgsConstructor
public class FireboltEngineService {
  private final FireboltAccountClient fireboltAccountClient;

  public String getEngineHost(
      String host, String dbName, String engineName, String account, String accessToken)
      throws FireboltException {
    String accountId = null;
    try {
      if (StringUtils.isNotEmpty(account)) {
        accountId = fireboltAccountClient.getAccountId(host, account, accessToken).orElse(null);
      }
      if (StringUtils.isEmpty(engineName))
        return fireboltAccountClient.getDbDefaultEngineAddress(
            host, accountId, dbName, accessToken);
      String engineID = fireboltAccountClient.getEngineId(host, accountId, engineName, accessToken);
      return fireboltAccountClient.getEngineAddress(
          host, accountId, engineName, engineID, accessToken);
    } catch (Exception e) {
      throw new FireboltException(String.format("Could not get engine host at %s", host), e);
    }
  }
}
