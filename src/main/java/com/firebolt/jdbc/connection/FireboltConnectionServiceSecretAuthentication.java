package com.firebolt.jdbc.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.FireboltObjectMapper;
import com.firebolt.jdbc.client.account.FireboltAccount;
import com.firebolt.jdbc.client.account.FireboltAccountClient;
import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.client.authentication.AuthenticationRequest;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.client.authentication.ServiceAccountAuthenticationRequest;
import com.firebolt.jdbc.client.gateway.GatewayUrlResponse;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltAccountIdService;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineService;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import com.firebolt.jdbc.service.FireboltStatementService;
import lombok.NonNull;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import static java.lang.String.format;

public class FireboltConnectionServiceSecretAuthentication extends FireboltConnection {
    private final FireboltGatewayUrlService fireboltGatewayUrlService;
    private final FireboltAccountIdService fireboltAccountIdService;
    private final FireboltEngineService fireboltEngineService;

    FireboltConnectionServiceSecretAuthentication(@NonNull String url, Properties connectionSettings, FireboltAuthenticationService fireboltAuthenticationService, FireboltGatewayUrlService fireboltGatewayUrlService, FireboltStatementService fireboltStatementService, FireboltEngineService fireboltEngineService, FireboltAccountIdService fireboltAccountIdService) throws SQLException {
        super(url, connectionSettings, fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService, fireboltAccountIdService);
        this.fireboltGatewayUrlService = fireboltGatewayUrlService;
        this.fireboltAccountIdService = fireboltAccountIdService;
        this.fireboltEngineService = fireboltEngineService;
        connect();
    }

    FireboltConnectionServiceSecretAuthentication(@NonNull String url, Properties connectionSettings) throws SQLException {
        super(url, connectionSettings);
        OkHttpClient httpClient = getHttpClient(loginProperties);
        ObjectMapper objectMapper = FireboltObjectMapper.getInstance();
        this.fireboltGatewayUrlService = new FireboltGatewayUrlService(createFireboltAccountRetriever(httpClient, objectMapper, "engineUrl", GatewayUrlResponse.class));
        this.fireboltAccountIdService = new FireboltAccountIdService(createFireboltAccountRetriever(httpClient, objectMapper, "resolve", FireboltAccount.class));
        this.fireboltEngineService = new FireboltEngineService(this, new FireboltAccountClient(httpClient, objectMapper, this, loginProperties.getUserDrivers(), loginProperties.getUserClients()));
        connect();
    }

    private <T> FireboltAccountRetriever<T> createFireboltAccountRetriever(OkHttpClient httpClient, ObjectMapper objectMapper, String path, Class<T> type) {
        return new FireboltAccountRetriever<>(httpClient, objectMapper, this, loginProperties.getUserDrivers(), loginProperties.getUserClients(), loginProperties.getHost(), path, type);
    }

    @Override
    protected void authenticate() throws SQLException {
        String account = loginProperties.getAccount();
        if (account == null) {
            throw new FireboltException("Cannot connect: account is missing");
        }
        String accessToken = getAccessToken(loginProperties).orElse(StringUtils.EMPTY);
        sessionProperties = getSessionPropertiesForSystemEngine(accessToken, account);
        if (loginProperties.isSystemEngine()) {
            assertDatabaseExisting(loginProperties.getDatabase());
        } else {
            sessionProperties = getSessionPropertiesForNonSystemEngine();
        }
    }

    private FireboltProperties getSessionPropertiesForNonSystemEngine() throws SQLException {
        sessionProperties = sessionProperties.toBuilder().engine(loginProperties.getEngine()).build();
        Engine engine = fireboltEngineService.getEngine(loginProperties.getEngine(), loginProperties.getDatabase());
        return loginProperties.toBuilder().host(engine.getEndpoint()).engine(engine.getName()).systemEngine(false).database(engine.getDatabase()).build();
    }

    @Override
    protected void assertDatabaseExisting(String database) throws SQLException {
        if (database !=  null && !fireboltEngineService.doesDatabaseExist(database)) {
            throw new FireboltException(format("Database %s does not exist", database));
        }
    }

    private FireboltProperties getSessionPropertiesForSystemEngine(String accessToken, String account) throws FireboltException {
        String systemEngineEndpoint = fireboltGatewayUrlService.getUrl(accessToken, account);
        String accountId = fireboltAccountIdService.getValue(accessToken, account);
        return this.loginProperties
                .toBuilder()
                .systemEngine(true)
                .additionalProperties(Map.of())
                .compress(false)
                .accountId(accountId)
                .host(UrlUtil.createUrl(systemEngineEndpoint).getHost()).database(null).build();
    }

    @Override
    protected FireboltAuthenticationClient createFireboltAuthenticationClient(OkHttpClient httpClient, ObjectMapper objectMapper) {
        return new FireboltAuthenticationClient(httpClient, objectMapper, this, loginProperties.getUserDrivers(), loginProperties.getUserClients()) {
            @Override
            public AuthenticationRequest getAuthenticationRequest(String username, String password, String host, String environment) {
                return new ServiceAccountAuthenticationRequest(username, password, environment);
            }
        };
    }

}
