package com.firebolt.jdbc.service;

import com.firebolt.jdbc.client.account.FireboltAccount;
import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.exception.FireboltException;

import java.sql.SQLException;

public class FireboltAccountIdService {
    private final FireboltAccountRetriever<FireboltAccount> firebolAccountClient;

    public FireboltAccountIdService(FireboltAccountRetriever<FireboltAccount> firebolAccountClient) {
        this.firebolAccountClient = firebolAccountClient;
    }

    public FireboltAccount getValue(String accessToken, String account) throws SQLException {
        return firebolAccountClient.retrieve(accessToken, account);
    }
}
