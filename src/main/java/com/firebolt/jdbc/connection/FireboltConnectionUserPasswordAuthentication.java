package com.firebolt.jdbc.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.FireboltObjectMapper;
import com.firebolt.jdbc.client.account.FireboltAccountClient;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.connection.settings.FireboltSessionProperty;
import com.firebolt.jdbc.service.FireboltAccountIdService;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
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

    FireboltConnectionUserPasswordAuthentication(@NonNull String url, Properties connectionSettings, FireboltAuthenticationService fireboltAuthenticationService, FireboltGatewayUrlService fireboltGatewayUrlService, FireboltStatementService fireboltStatementService, FireboltEngineService fireboltEngineService, FireboltAccountIdService fireboltAccountIdService) throws SQLException {
        super(url, connectionSettings, fireboltAuthenticationService, fireboltGatewayUrlService, fireboltStatementService, fireboltEngineService, fireboltAccountIdService, 1);
        this.fireboltEngineService = fireboltEngineService;
        connect();
    }

    FireboltConnectionUserPasswordAuthentication(@NonNull String url, Properties connectionSettings) throws SQLException {
        super(url, connectionSettings, 1);
        OkHttpClient httpClient = getHttpClient(loginProperties);
        ObjectMapper objectMapper = FireboltObjectMapper.getInstance();
        this.fireboltEngineService = new FireboltEngineService(this, new FireboltAccountClient(httpClient, objectMapper, this, loginProperties.getUserDrivers(), loginProperties.getUserClients()));
        connect();
    }

    @Override
    protected void authenticate() throws SQLException {
        String accessToken = getAccessToken(loginProperties).orElse(StringUtils.EMPTY);
        String endpoint = fireboltEngineService.getEngine(httpConnectionUrl, loginProperties, accessToken).getEndpoint();
        this.sessionProperties = loginProperties.toBuilder().host(endpoint).build();
    }

    @Override
    protected FireboltProperties extractFireboltProperties(String jdbcUri, Properties connectionProperties) {
        FireboltProperties properties = super.extractFireboltProperties(jdbcUri, connectionProperties);
        boolean systemEngine = "system".equals(properties.getEngine());
        return properties.toBuilder().systemEngine(systemEngine).build();
    }

    @Override
    protected void assertDatabaseExisting(String database) throws SQLException {

    }
}
