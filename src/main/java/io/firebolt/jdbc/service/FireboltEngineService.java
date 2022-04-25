package io.firebolt.jdbc.service;

import io.firebolt.jdbc.client.account.FireboltAccountClient;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

@RequiredArgsConstructor
public class FireboltEngineService {
    private final FireboltAccountClient fireboltAccountClient;

    public String getEngineAddress(String host, String dbName, String engineName, String account, String accessToken) throws IOException {
        String accountId = null;
        if (StringUtils.isNotEmpty(account))
            accountId = fireboltAccountClient.getAccountId(host, account, accessToken).orElse(null);
        if (StringUtils.isEmpty(engineName))
            return fireboltAccountClient.getDbAddress(host, accountId, dbName, accessToken);
        String engineID = fireboltAccountClient.getEngineId(host, accountId, engineName, accessToken);
        return fireboltAccountClient.getEngineAddress(host, accountId, engineName, engineID, accessToken);
    }
}
