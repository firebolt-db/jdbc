package com.firebolt.jdbc.service;

import com.firebolt.jdbc.client.account.FireboltAccount;
import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.exception.FireboltException;

public class FireboltAccountIdService {
    private final FireboltAccountRetriever<FireboltAccount> firebolAccountClient;

    public FireboltAccountIdService(FireboltAccountRetriever<FireboltAccount> firebolAccountClient) {
        this.firebolAccountClient = firebolAccountClient;
    }

    public String getValue(String accessToken, String account) throws FireboltException {
        return firebolAccountClient.retrieve(accessToken, account).getId();
    }
}
