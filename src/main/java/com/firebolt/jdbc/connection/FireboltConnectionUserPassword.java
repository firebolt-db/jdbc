package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.client.account.FireboltAccountClient;
import com.firebolt.jdbc.client.authentication.AuthenticationRequest;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.client.authentication.UsernamePasswordAuthenticationRequest;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineApiService;
import com.firebolt.jdbc.service.FireboltEngineInformationSchemaService;
import com.firebolt.jdbc.service.FireboltEngineService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.type.ParserVersion;
import lombok.NonNull;
import okhttp3.OkHttpClient;

import java.sql.SQLException;
import java.util.Properties;

public class FireboltConnectionUserPassword extends FireboltConnection {
    // Visible for testing
    public static final String SYSTEM_ENGINE_NAME = "system";
    private static final String PROTOCOL_VERSION = null; // It could be 1.0, but we send null for backwards compatibility, so not version header is sent
    private final FireboltEngineService fireboltEngineService;

    FireboltConnectionUserPassword(@NonNull String url,
                                   Properties connectionSettings,
                                   FireboltAuthenticationService fireboltAuthenticationService,
                                   FireboltStatementService fireboltStatementService,
            FireboltEngineInformationSchemaService fireboltEngineService,
            ParserVersion parserVersion) throws SQLException {
        super(url, connectionSettings, fireboltAuthenticationService, fireboltStatementService, PROTOCOL_VERSION,
                parserVersion);
        this.fireboltEngineService = fireboltEngineService;
        connect();
    }

    @ExcludeFromJacocoGeneratedReport
    FireboltConnectionUserPassword(@NonNull String url, Properties connectionSettings, ParserVersion parserVersion)
            throws SQLException {
        super(url, connectionSettings, PROTOCOL_VERSION, parserVersion);
        OkHttpClient httpClient = getHttpClient(loginProperties);
        this.fireboltEngineService = new FireboltEngineApiService(new FireboltAccountClient(httpClient, this, loginProperties.getUserDrivers(), loginProperties.getUserClients()));
        connect();
    }

    @Override
    protected void authenticate() throws SQLException {
        String accessToken = getAccessToken(loginProperties).orElse("");
        FireboltProperties propertiesWithAccessToken = loginProperties.toBuilder().accessToken(accessToken).build();
        Engine engine = fireboltEngineService.getEngine(propertiesWithAccessToken);
        String database = loginProperties.getDatabase();
        if (engine.getDatabase() != null) {
           database = engine.getDatabase();
        }
        sessionProperties = loginProperties.toBuilder().host(engine.getEndpoint()).engine(engine.getName()).database(database).build();
    }

    @Override
    protected FireboltProperties extractFireboltProperties(String jdbcUri, Properties connectionProperties) {
        FireboltProperties properties = super.extractFireboltProperties(jdbcUri, connectionProperties);
        boolean systemEngine = SYSTEM_ENGINE_NAME.equals(properties.getEngine());
        boolean compressed = !systemEngine && properties.isCompress();
        return properties.toBuilder().systemEngine(systemEngine).compress(compressed).build();
    }

    @Override
    protected void assertDatabaseExisting(String database) {
        // empty implementation. There is no way to validate that DB exists. Even if such API exists it is irrelevant
        // because it is used for old DB that will be obsolete soon and only when using either system or local engine.
    }

    @Override
    protected FireboltAuthenticationClient createFireboltAuthenticationClient(OkHttpClient httpClient) {
        return new FireboltAuthenticationClient(httpClient,this, loginProperties.getUserDrivers(), loginProperties.getUserClients()) {
            @Override
            public AuthenticationRequest getAuthenticationRequest(String username, String password, String host, String environment) {
                return new UsernamePasswordAuthenticationRequest(username, password, host);
            }
        };
    }
}
