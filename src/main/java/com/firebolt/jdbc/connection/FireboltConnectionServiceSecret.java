package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.client.authentication.AuthenticationRequest;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.client.authentication.ServiceAccountAuthenticationRequest;
import com.firebolt.jdbc.client.gateway.GatewayUrlResponse;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineInformationSchemaService;
import com.firebolt.jdbc.service.FireboltEngineService;
import com.firebolt.jdbc.service.FireboltEngineVersion2Service;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.type.ParserVersion;
import java.net.URL;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import lombok.NonNull;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;

import static com.firebolt.jdbc.exception.ExceptionType.RESOURCE_NOT_FOUND;
import static java.lang.String.format;

public class FireboltConnectionServiceSecret extends FireboltConnection {
    private static final String PROTOCOL_VERSION = "2.1";
    private final FireboltGatewayUrlService fireboltGatewayUrlService;
    private FireboltEngineService fireboltEngineService; // depends on infra version and is discovered during authentication

    FireboltConnectionServiceSecret(@NonNull String url,
                                    Properties connectionSettings,
                                    FireboltAuthenticationService fireboltAuthenticationService,
                                    FireboltGatewayUrlService fireboltGatewayUrlService,
                                    FireboltStatementService fireboltStatementService,
                                    FireboltEngineInformationSchemaService fireboltEngineService,
                                    ParserVersion parserVersion) throws SQLException {
        super(url, connectionSettings, fireboltAuthenticationService, fireboltStatementService, PROTOCOL_VERSION,
                parserVersion);
        this.fireboltGatewayUrlService = fireboltGatewayUrlService;
        this.fireboltEngineService = fireboltEngineService;
        connect();
    }

    @ExcludeFromJacocoGeneratedReport
    FireboltConnectionServiceSecret(@NonNull String url, Properties connectionSettings, ParserVersion parserVersion)
            throws SQLException {
        super(url, connectionSettings, PROTOCOL_VERSION, parserVersion);
        OkHttpClient httpClient = getHttpClient(loginProperties);
        this.fireboltGatewayUrlService = new FireboltGatewayUrlService(createFireboltAccountRetriever(httpClient, GatewayUrlResponse.class));
        connect();
    }

    private <T> FireboltAccountRetriever<T> createFireboltAccountRetriever(OkHttpClient httpClient, Class<T> type) {
        return new FireboltAccountRetriever<>(httpClient, this, loginProperties.getUserDrivers(), loginProperties.getUserClients(), loginProperties.getHost(), type);
    }

    @Override
    protected void authenticate() throws SQLException {
        String accessToken = getAccessToken(loginProperties).orElse("");
        sessionProperties = getSessionPropertiesForSystemEngine(accessToken, loginProperties.getAccount());
        assertDatabaseExisting(loginProperties.getDatabase());
        if (!loginProperties.isSystemEngine()) {
            sessionProperties = getSessionPropertiesForNonSystemEngine();
        }
    }

    /**
     * Perform syntactic validation for the mandatory connection params which are:
     * - clientId
     * - clientSecret
     * - account
     *
     * Also make sure the access token is not passed in
     */
    protected void validateConnectionParameters() throws FireboltException {
        String account = loginProperties.getAccount();
        if (StringUtils.isBlank(account)) {
            throw new FireboltException("Cannot connect: account is missing");
        }

        String clientId = loginProperties.getPrincipal();
        if (StringUtils.isBlank(clientId)) {
            throw new FireboltException("Cannot connect: clientId is missing");
        }

        String clientSecret = loginProperties.getSecret();
        if (StringUtils.isBlank(clientSecret)) {
            throw new FireboltException("Cannot connect: clientSecret is missing");
        }

        // make sure the access token is not passed in
        String accessToken = loginProperties.getAccessToken();
        if (StringUtils.isNotBlank(accessToken)) {
            throw new FireboltException("Ambiguity: Both access token and client ID/secret are supplied");
        }

    }

    private FireboltProperties getSessionPropertiesForNonSystemEngine() throws SQLException {
        sessionProperties = sessionProperties.toBuilder().engine(loginProperties.getEngine()).build();
        Engine engine = getFireboltEngineService().getEngine(loginProperties);
        // update Firebolt properties. If we are here there are no contradictions between discovered and supplied parameters (db or engine): all validations are done in getEngine()
        return loginProperties.toBuilder()
                .host(engine.getEndpoint()) // was not know until this point
                .engine(engine.getName()) // engine name is updated here because this code is running either if engine has been supplied in initial parameters or when default engine for current DB was discovered
                .systemEngine(false) // this is definitely not system engine
                .database(engine.getDatabase()) // DB is updated because this code is running either when DB was supplied in initial parameters or not
                .accountId(sessionProperties.getAccountId()) // discovered in case of v2 engine
                .runtimeAdditionalProperties(sessionProperties.getRuntimeAdditionalProperties()) // discovered in case of v2 engine
                .build();
    }

    private void assertDatabaseExisting(String database) throws SQLException {
        if (database != null && !getFireboltEngineService().doesDatabaseExist(database)) {
            throw new FireboltException(format("Database %s does not exist", database), RESOURCE_NOT_FOUND);
        }
    }

    private FireboltProperties getSessionPropertiesForSystemEngine(String accessToken, String accountName) throws SQLException {
        String systemEngineEndpoint = fireboltGatewayUrlService.getUrl(accessToken, accountName);
        infraVersion = 2;
        URL systemEngienUrl = UrlUtil.createUrl(systemEngineEndpoint);
        Map<String, String> systemEngineUrlUrlParams = UrlUtil.getQueryParameters(systemEngienUrl);
        for (Entry<String, String> e : systemEngineUrlUrlParams.entrySet()) {
            loginProperties.addProperty(e);
        }
        return loginProperties
                .toBuilder()
                .systemEngine(true)
                .compress(false)
                .host(systemEngienUrl.getHost())
                .build();
    }




    private FireboltEngineService getFireboltEngineService() throws SQLException {
        if (fireboltEngineService == null) {
            int currentInfraVersion = Optional.ofNullable(loginProperties.getAdditionalProperties().get("infraVersion")).map(Integer::parseInt).orElse(infraVersion);
            fireboltEngineService = currentInfraVersion >= 2 ? new FireboltEngineVersion2Service(this) : new FireboltEngineInformationSchemaService(this);
        }
        return fireboltEngineService;
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
