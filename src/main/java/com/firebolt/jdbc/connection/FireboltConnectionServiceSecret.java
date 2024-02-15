package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.client.account.FireboltAccount;
import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.client.authentication.AuthenticationRequest;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.client.authentication.ServiceAccountAuthenticationRequest;
import com.firebolt.jdbc.client.gateway.GatewayUrlResponse;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltAccountIdService;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineInformationSchemaService;
import com.firebolt.jdbc.service.FireboltEngineService;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import com.firebolt.jdbc.service.FireboltStatementService;
import lombok.NonNull;
import okhttp3.OkHttpClient;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

import static com.firebolt.jdbc.exception.ExceptionType.RESOURCE_NOT_FOUND;
import static java.lang.String.format;

public class FireboltConnectionServiceSecret extends FireboltConnection {
    private static final String PROTOCOL_VERSION = "2.0";
    private final FireboltGatewayUrlService fireboltGatewayUrlService;
    private final FireboltAccountIdService fireboltAccountIdService;
    private final FireboltEngineService fireboltEngineService;

    FireboltConnectionServiceSecret(@NonNull String url,
                                    Properties connectionSettings,
                                    FireboltAuthenticationService fireboltAuthenticationService,
                                    FireboltGatewayUrlService fireboltGatewayUrlService,
                                    FireboltStatementService fireboltStatementService,
                                    FireboltEngineInformationSchemaService fireboltEngineService,
                                    FireboltAccountIdService fireboltAccountIdService) throws SQLException {
        super(url, connectionSettings, fireboltAuthenticationService, fireboltStatementService, PROTOCOL_VERSION);
        this.fireboltGatewayUrlService = fireboltGatewayUrlService;
        this.fireboltAccountIdService = fireboltAccountIdService;
        this.fireboltEngineService = fireboltEngineService;
        connect();
    }

    @ExcludeFromJacocoGeneratedReport
    FireboltConnectionServiceSecret(@NonNull String url, Properties connectionSettings) throws SQLException {
        super(url, connectionSettings, PROTOCOL_VERSION);
        OkHttpClient httpClient = getHttpClient(loginProperties);
        this.fireboltGatewayUrlService = new FireboltGatewayUrlService(createFireboltAccountRetriever(httpClient,"engineUrl", GatewayUrlResponse.class));
        this.fireboltAccountIdService = new FireboltAccountIdService(createFireboltAccountRetriever(httpClient,"resolve", FireboltAccount.class));
        this.fireboltEngineService = new FireboltEngineInformationSchemaService(this);
        connect();
    }

    private <T> FireboltAccountRetriever<T> createFireboltAccountRetriever(OkHttpClient httpClient, String path, Class<T> type) {
        return new FireboltAccountRetriever<>(httpClient, this, loginProperties.getUserDrivers(), loginProperties.getUserClients(), loginProperties.getHost(), path, type);
    }

    @Override
    protected void authenticate() throws SQLException {
        String account = loginProperties.getAccount();
        if (account == null) {
            throw new FireboltException("Cannot connect: account is missing");
        }
        if (loginProperties.isSystemEngine() && loginProperties.getDatabase() == null) {
            throw new FireboltException("The database with the name null could not be found", RESOURCE_NOT_FOUND);
        }
        String accessToken = getAccessToken(loginProperties).orElse("");
        sessionProperties = getSessionPropertiesForSystemEngine(accessToken, account);
        assertDatabaseExisting(loginProperties.getDatabase());
        if (!loginProperties.isSystemEngine()) {
            sessionProperties = getSessionPropertiesForNonSystemEngine();
        }
    }

    private FireboltProperties getSessionPropertiesForNonSystemEngine() throws SQLException {
        sessionProperties = sessionProperties.toBuilder().engine(loginProperties.getEngine()).build();
        Engine engine = fireboltEngineService.getEngine(loginProperties);
        // update Firebolt properties. If we are here there are no contradictions between discovered and supplied parameters (db or engine): all validations are done in getEngine()
        return loginProperties.toBuilder()
                .host(engine.getEndpoint()) // was not know until this point
                .engine(engine.getName()) // engine name is updated here because this code is running either if engine has been supplied in initial parameters or when default engine for current DB was discovered
                .systemEngine(false) // this is definitely not system engine
                .database(engine.getDatabase()) // DB is updated because this code is running either when DB was supplied in initial parameters or not
                .build();
    }

    @Override
    protected void assertDatabaseExisting(String database) throws SQLException {
        if (database != null && !fireboltEngineService.doesDatabaseExist(database)) {
            throw new FireboltException(format("Database %s does not exist", database), RESOURCE_NOT_FOUND);
        }
    }

    private FireboltProperties getSessionPropertiesForSystemEngine(String accessToken, String account) throws FireboltException {
        String systemEngineEndpoint = fireboltGatewayUrlService.getUrl(accessToken, account);
        String accountId = fireboltAccountIdService.getValue(accessToken, account);
        return loginProperties
                .toBuilder()
                .systemEngine(true)
                .additionalProperties(new HashMap<>()) // additional properties must be writable
                .compress(false)
                .accountId(accountId)
                .host(UrlUtil.createUrl(systemEngineEndpoint).getHost())
                .build();
    }

    @Override
    protected FireboltProperties extractFireboltProperties(String jdbcUri, Properties connectionProperties) {
        FireboltProperties properties = super.extractFireboltProperties(jdbcUri, connectionProperties);
        if ("".equals(properties.getDatabase())) {
            return properties.toBuilder().database(null).build();
        }
        return properties;
    }


    @Override
    protected FireboltAuthenticationClient createFireboltAuthenticationClient(OkHttpClient httpClient) {
        return new FireboltAuthenticationClient(httpClient, this, loginProperties.getUserDrivers(), loginProperties.getUserClients()) {
            @Override
            public AuthenticationRequest getAuthenticationRequest(String username, String password, String host, String environment) {
                return new ServiceAccountAuthenticationRequest(username, password, environment);
            }
        };
    }

}
