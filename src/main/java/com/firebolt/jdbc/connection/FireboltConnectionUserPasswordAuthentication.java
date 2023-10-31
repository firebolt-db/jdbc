package com.firebolt.jdbc.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.FireboltObjectMapper;
import com.firebolt.jdbc.client.account.FireboltAccountClient;
import com.firebolt.jdbc.client.authentication.AuthenticationRequest;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.client.authentication.OldServiceAccountAuthenticationRequest;
import com.firebolt.jdbc.client.authentication.UsernamePasswordAuthenticationRequest;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.service.FireboltAccountIdService;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineApiService;
import com.firebolt.jdbc.service.FireboltEngineInformationSchemaService;
import com.firebolt.jdbc.service.FireboltEngineService;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import com.firebolt.jdbc.service.FireboltStatementService;
import lombok.NonNull;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.Properties;

public class FireboltConnectionUserPasswordAuthentication extends FireboltConnection {
    private final FireboltEngineService fireboltEngineService;

    FireboltConnectionUserPasswordAuthentication(@NonNull String url, Properties connectionSettings, FireboltAuthenticationService fireboltAuthenticationService, FireboltGatewayUrlService fireboltGatewayUrlService, FireboltStatementService fireboltStatementService, FireboltEngineInformationSchemaService fireboltEngineService, FireboltAccountIdService fireboltAccountIdService) throws SQLException {
        super(url, connectionSettings, fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService, fireboltAccountIdService);
        this.fireboltEngineService = fireboltEngineService;
        connect();
    }

    FireboltConnectionUserPasswordAuthentication(@NonNull String url, Properties connectionSettings) throws SQLException {
        super(url, connectionSettings);
        OkHttpClient httpClient = getHttpClient(loginProperties);
        ObjectMapper objectMapper = FireboltObjectMapper.getInstance();
        this.fireboltEngineService = new FireboltEngineApiService(new FireboltAccountClient(httpClient, objectMapper, this, loginProperties.getUserDrivers(), loginProperties.getUserClients()));
        connect();
    }

    @Override
    protected void authenticate() throws SQLException {
        String accessToken = getAccessToken(loginProperties).orElse(StringUtils.EMPTY);
        FireboltProperties propertiesWithAccessToken = loginProperties.toBuilder().accessToken(accessToken).build();
        String endpoint = fireboltEngineService.getEngine(propertiesWithAccessToken).getEndpoint();
        this.sessionProperties = loginProperties.toBuilder().host(endpoint).build();
    }

    @Override
    protected FireboltProperties extractFireboltProperties(String jdbcUri, Properties connectionProperties) throws SQLException {
        FireboltProperties properties = super.extractFireboltProperties(jdbcUri, connectionProperties);
        boolean systemEngine = "system".equals(properties.getEngine());
        return properties.toBuilder().systemEngine(systemEngine).build();
    }

    @Override
    protected void assertDatabaseExisting(String database) throws SQLException {

    }

    @Override
    protected FireboltAuthenticationClient createFireboltAuthenticationClient(OkHttpClient httpClient, ObjectMapper objectMapper) {
        return new FireboltAuthenticationClient(httpClient, objectMapper, this, loginProperties.getUserDrivers(), loginProperties.getUserClients()) {
            @Override
            public AuthenticationRequest getAuthenticationRequest(String username, String password, String host, String environment) {
                if (StringUtils.isEmpty(username) || StringUtils.contains(username, "@")) {
                    return new UsernamePasswordAuthenticationRequest(username, password, host);
                } else {
                    return new OldServiceAccountAuthenticationRequest(username, password, host);
                }
            }
        };
    }
}
