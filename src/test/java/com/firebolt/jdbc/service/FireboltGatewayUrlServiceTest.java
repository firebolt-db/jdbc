package com.firebolt.jdbc.service;

import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.client.gateway.GatewayUrlResponse;
import com.firebolt.jdbc.exception.FireboltException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FireboltGatewayUrlServiceTest {
    @ParameterizedTest
    @CsvSource({
            "http://host,http://host",
            "https://host,https://host",
            "host,https://host", // force https
            "http://host?name=value,http://host?name=value",
            "https://host?name=value,https://host?name=value",
            "host?name=value,https://host?name=value",
    })
    void test(String rawUrl, String expectedUrl) throws SQLException {
        @SuppressWarnings("unchecked")
        FireboltAccountRetriever<GatewayUrlResponse> fireboltGatewayUrlClient = mock(FireboltAccountRetriever.class);
        when(fireboltGatewayUrlClient.retrieve("token", "account")).thenReturn(new GatewayUrlResponse(rawUrl));
        String actualUrl = new FireboltGatewayUrlService(fireboltGatewayUrlClient).getUrl("token", "account");
        assertEquals(expectedUrl, actualUrl);
    }

}