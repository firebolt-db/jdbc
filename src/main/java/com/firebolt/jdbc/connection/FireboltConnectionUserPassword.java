package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.FireboltBackendType;
import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.client.account.FireboltAccountClient;
import com.firebolt.jdbc.client.authentication.AuthenticationRequest;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.client.authentication.UsernamePasswordAuthenticationRequest;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineApiService;
import com.firebolt.jdbc.service.FireboltEngineInformationSchemaService;
import com.firebolt.jdbc.service.FireboltEngineService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.type.ParserVersion;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import lombok.CustomLog;
import lombok.NonNull;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;

@CustomLog
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
    protected void validateConnectionParameters() throws SQLException {
        String username = loginProperties.getPrincipal();
        if (StringUtils.isBlank(username)) {
            throw new FireboltException("Cannot connect: username is missing");
        }

        String password = loginProperties.getSecret();
        if (StringUtils.isBlank(password)) {
            throw new FireboltException("Cannot connect: password is missing");
        }

        // make sure the access token is not passed in
        String accessToken = loginProperties.getAccessToken();
        if (StringUtils.isNotBlank(accessToken)) {
            throw new FireboltException("Ambiguity: Both access token and username/password are provided");
        }

        // check to see if the connection was set with a connection caching
        if (loginProperties.isConnectionCachingEnabled()) {
            log.warn("The cache_connection parameter is only supported with Firebolt 2.0. Your connections will not be cached");
        }

    }

    @Override
    protected boolean isConnectionCachingEnabled() {
        // check to see if the connection was set with a connection caching
        if (loginProperties.isConnectionCachingEnabled()) {
            log.warn("The cache_connection parameter is only supported with Firebolt 2.0. Your connections will not be cached");
        }

        return false;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        validateConnectionIsNotClose();
        return true;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    @NotImplemented
    public void setAutoCommit(boolean autoCommit) throws SQLException {}

    @Override
    public int getTransactionIsolation() throws SQLException {
        validateConnectionIsNotClose();
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (level != Connection.TRANSACTION_NONE) {
            throw new FireboltSQLFeatureNotSupportedException();
        }
    }

    @Override
    @NotImplemented
    public void commit() throws  SQLException {
        // no-op as transactions are not supported
    }

    @Override
    @NotImplemented
    public void rollback() throws SQLException {
        // no-op as transactions are not supported
    }

    /**
     * Ensures a transaction is started if auto-commit is disabled and no transaction is active.
     * Called automatically before query execution.
     *
     *
     * @throws SQLException if there's an error starting the transaction
     */
    @Override
    public void ensureTransactionForQueryExecution() throws SQLException {
        /* Since v1 does not support transactions this will be no-op */
    }
    @Override
    public Optional<String> getConnectionUserAgentHeader() {
        return Optional.empty();
    }

    @Override
    public FireboltBackendType getBackendType() {
        return FireboltBackendType.CLOUD_1_0;
    }

    @Override
    protected FireboltProperties extractFireboltProperties(String jdbcUri, Properties connectionProperties) {
        FireboltProperties properties = super.extractFireboltProperties(jdbcUri, connectionProperties);
        boolean systemEngine = SYSTEM_ENGINE_NAME.equals(properties.getEngine());
        boolean compressed = !systemEngine && properties.isCompress();
        return properties.toBuilder().systemEngine(systemEngine).compress(compressed).build();
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

    @Override
    public int getInfraVersion() {
        return 1;
    }
}
