package com.firebolt.jdbc.service;

import com.firebolt.jdbc.client.gateway.FireboltGatewayUrlClient;
import com.firebolt.jdbc.exception.FireboltException;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class FireboltGatewayUrlService {

    private final FireboltGatewayUrlClient fireboltGatewayUrlClient;

    public String getUrl(String accessToken, String account) throws FireboltException {
        return fireboltGatewayUrlClient.getGatewayUrl(accessToken, account).getEngineUrl();
    }
}
