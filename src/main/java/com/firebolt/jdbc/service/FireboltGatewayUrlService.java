package com.firebolt.jdbc.service;

import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.client.gateway.GatewayUrlResponse;
import com.firebolt.jdbc.exception.FireboltException;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class FireboltGatewayUrlService {

    private final FireboltAccountRetriever<GatewayUrlResponse> fireboltGatewayUrlClient;

    public String getUrl(String accessToken, String account) throws FireboltException {
        return fireboltGatewayUrlClient.retrieve(accessToken, account).getEngineUrl();
    }
}
