package com.firebolt.jdbc.service;

import com.firebolt.jdbc.client.account.FireboltAccount;
import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FireboltAccountIdServiceTest {
    @Test
    void getValue() throws SQLException {
        FireboltAccountRetriever<FireboltAccount> firebolAccountClient = mock(FireboltAccountRetriever.class);
        FireboltAccount account = new FireboltAccount("id", "region", 123);
        when(firebolAccountClient.retrieve("token", "account")).thenReturn(account);
        assertSame(account, new FireboltAccountIdService(firebolAccountClient).getValue("token", "account"));
    }
}
