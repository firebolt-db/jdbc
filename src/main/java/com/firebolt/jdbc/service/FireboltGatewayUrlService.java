package com.firebolt.jdbc.service;

import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.client.gateway.GatewayUrlResponse;
import lombok.RequiredArgsConstructor;

import java.sql.SQLException;

@RequiredArgsConstructor
public class FireboltGatewayUrlService {

    private final FireboltAccountRetriever<GatewayUrlResponse> fireboltGatewayUrlClient;

    private String addProtocolToUrl(String url) {
        if (!url.startsWith("http")) {
            // assume secure connection
            url = "https://" + url;
        }
        return url;
    }

    public String getUrl(String accessToken, String account) throws SQLException {
        return addProtocolToUrl(fireboltGatewayUrlClient.retrieve(accessToken, account).getEngineUrl());
    }
}
