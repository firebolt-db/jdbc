package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.FireboltBackendType;
import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.cache.CacheService;
import com.firebolt.jdbc.cache.ConnectionCache;
import com.firebolt.jdbc.cache.key.CacheKey;
import com.firebolt.jdbc.cache.key.ClientSecretCacheKey;
import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.client.authentication.AuthenticationRequest;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.client.authentication.ServiceAccountAuthenticationRequest;
import com.firebolt.jdbc.client.gateway.GatewayUrlResponse;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineVersion2Service;
import com.firebolt.jdbc.service.FireboltGatewayUrlService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.type.ParserVersion;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import lombok.CustomLog;
import lombok.NonNull;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

@CustomLog
public class FireboltConnectionServiceSecret extends FireboltConnection {

    private static final String PROTOCOL_VERSION = "2.4";
    private final FireboltGatewayUrlService fireboltGatewayUrlService;
    private FireboltEngineVersion2Service fireboltEngineVersion2Service;
    private boolean autoCommit = true;
    private boolean inTransaction = false;
    private boolean executingTransactionCommand = false;

    private final CacheService cacheService;

    /**
     * If caching is enabled, then this value will contain the cached values
     */
    private ConnectionCache connectionCache;

    /**
     * Each connection will have its unique id
     */
    private String connectionId;


    FireboltConnectionServiceSecret(@NonNull Pair<String, Properties> urlConnectionParams,
                                    FireboltAuthenticationService fireboltAuthenticationService,
                                    FireboltGatewayUrlService fireboltGatewayUrlService,
                                    FireboltStatementService fireboltStatementService,
                                    FireboltEngineVersion2Service fireboltEngineVersion2Service,
                                    ConnectionIdGenerator connectionIdGenerator,
                                    CacheService cacheService) throws SQLException {
        super(urlConnectionParams.getKey(), urlConnectionParams.getValue(), fireboltAuthenticationService, fireboltStatementService, PROTOCOL_VERSION,
                ParserVersion.CURRENT);
        this.fireboltGatewayUrlService = fireboltGatewayUrlService;
        this.fireboltEngineVersion2Service = fireboltEngineVersion2Service;
        this.connectionId = connectionIdGenerator.generateId();
        this.cacheService = cacheService;
        connect();
    }

    @ExcludeFromJacocoGeneratedReport
    FireboltConnectionServiceSecret(@NonNull String url, Properties connectionSettings, ConnectionIdGenerator connectionIdGenerator, CacheService cacheService)
            throws SQLException {
        super(url, connectionSettings, PROTOCOL_VERSION, ParserVersion.CURRENT);
        OkHttpClient httpClient = getHttpClient(loginProperties);
        this.fireboltGatewayUrlService = new FireboltGatewayUrlService(createFireboltAccountRetriever(httpClient, GatewayUrlResponse.class));
        this.fireboltEngineVersion2Service = new FireboltEngineVersion2Service(this);
        this.cacheService = cacheService;
        this.connectionId = connectionIdGenerator.generateId();
        connect();
    }

    private <T> FireboltAccountRetriever<T> createFireboltAccountRetriever(OkHttpClient httpClient, Class<T> type) {
        return new FireboltAccountRetriever<>(httpClient, this, loginProperties.getUserDrivers(), loginProperties.getUserClients(), loginProperties.getHost(), type);
    }

    @Override
    protected void authenticate() throws SQLException {
        prepareCacheIfNeeded();

        // make sure the clientId/clientSecret is valid
        String accessToken = getAccessTokenInternal();

        // make sure account exists
        sessionProperties = getSessionPropertiesForSystemEngine(accessToken, loginProperties.getAccount());

        if (!loginProperties.isSystemEngine()) {
            sessionProperties = getSessionPropertiesForNonSystemEngine(loginProperties.getEngine());
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        validateConnectionIsNotClose();
        return autoCommit;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        validateConnectionIsNotClose();

        if (autoCommit && inTransaction) {
            commit();
        }
        this.autoCommit = autoCommit;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        validateConnectionIsNotClose();
        return Connection.TRANSACTION_REPEATABLE_READ;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        validateConnectionIsNotClose();
        if (level != Connection.TRANSACTION_REPEATABLE_READ) {
            throw new FireboltSQLFeatureNotSupportedException("Only TRANSACTION_REPEATABLE_READ isolation level is supported");
        }
    }

    @Override
    public void commit() throws SQLException {
        validateConnectionIsNotClose();
        if (autoCommit) {
            throw new FireboltException("Cannot commit when auto-commit is enabled");
        }
        if (!inTransaction) {
            throw new FireboltException("No transaction is currently active");
        }
        try {
            executeTransactionCommand("COMMIT");
            inTransaction = false;
        } catch (SQLException ex) {
            throw new FireboltException("Could not commit the transaction", ex);
        }
    }

    @Override
    public void rollback() throws SQLException {
        validateConnectionIsNotClose();
        if (autoCommit) {
            throw new FireboltException("Cannot rollback when auto-commit is enabled");
        }
        if (!inTransaction) {
            throw new FireboltException("No transaction is currently active");
        }
        try {
            executeTransactionCommand("ROLLBACK");
            inTransaction = false;
        } catch (SQLException ex) {
            throw new FireboltException("Could not rollback the transaction", ex);
        }
    }

    /**
     * Ensures a transaction is started if auto-commit is disabled and no transaction is active.
     * Called automatically before query execution.
     *
     * @throws SQLException if there's an error starting the transaction
     */
    @Override
    public void ensureTransactionForQueryExecution() throws SQLException {
        validateConnectionIsNotClose();

        if (executingTransactionCommand || autoCommit) {
            return;
        }

        if (!inTransaction) {
            try {
                executeTransactionCommand("BEGIN TRANSACTION");
                inTransaction = true;
                log.debug("Auto-started transaction for query execution");
            } catch (SQLException ex) {
                throw new FireboltException("Could not start transaction for query execution", ex);
            }
        }
    }

    private void executeTransactionCommand(String sql) throws SQLException {
        executingTransactionCommand = true;
        try (Statement statement = createStatement()) {
            statement.execute(sql);
        } finally {
            executingTransactionCommand = false;
        }
    }

    private String getAccessTokenInternal() throws SQLException {
        if (!isConnectionCachingEnabled()) {
            return getAccessToken(loginProperties).orElse("");
        } else {
            // caching is enabled so check the cache first
            String accessTokenFromCache = connectionCache.getAccessToken();
            if (StringUtils.isNotBlank(accessTokenFromCache)) {
                return accessTokenFromCache;
            }

            // not in cache so get it from the source.
            synchronized (connectionCache) {
                String accessTokenFromFirebolt = getAccessToken(loginProperties).orElse("");

                // only save it in cache if not empty
                if (StringUtils.isNotBlank(accessTokenFromFirebolt)) {
                    connectionCache.setAccessToken(accessTokenFromFirebolt);
                    cacheService.put(getCacheKey(), connectionCache);
                }

                return accessTokenFromFirebolt;
            }
        }
    }

    /**
     * If there is no entry in the cache with this key, add one if caching is enabled
     */
    protected void prepareCacheIfNeeded() {
        if (!isConnectionCachingEnabled()) {
            return;
        }

        CacheKey key = getCacheKey();
        Optional<ConnectionCache> connectionCacheOptional = cacheService.get(key);
        if (connectionCacheOptional.isPresent()) {
            this.connectionCache = connectionCacheOptional.get();
            return;
        }

        synchronized (cacheService) {
            // check again to make sure another thread did not already set the cache value
            connectionCacheOptional = cacheService.get(key);
            if (connectionCacheOptional.isPresent()) {
                this.connectionCache = connectionCacheOptional.get();
                return;
            }

            // no connection in cache so set a fresh connection
            this.connectionCache = new ConnectionCache(connectionId);
            cacheService.put(key, connectionCache);
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

    @Override
    protected boolean isConnectionCachingEnabled() {
        return Boolean.valueOf(loginProperties.isConnectionCachingEnabled());
    }

    @Override
    public Optional<String> getConnectionUserAgentHeader() {
        if (!isConnectionCachingEnabled()) {
            return Optional.empty();
        }

        // connection is cached so add the connection info
        StringBuilder additionalUserAgentHeaderValue = new StringBuilder("connId:").append(connectionId);

        // if the current connectionId is not the same with connection cache, it means that the connection was cached
        if (!connectionId.equals(connectionCache.getConnectionId())) {
            additionalUserAgentHeaderValue.append("; cachedConnId:").append(connectionCache.getConnectionId())
                    .append("-").append(connectionCache.getCacheSource());
        }

        return Optional.of(additionalUserAgentHeaderValue.toString());
    }

    @Override
    public FireboltBackendType getBackendType() {
        return FireboltBackendType.CLOUD_2_0;
    }

    protected CacheKey getCacheKey() {
        // cache key is formed by clientId/clientSecret and account.
        // When this method is called the connection parameters have already been validated syntactically
        String clientId = loginProperties.getPrincipal();
        String clientSecret = loginProperties.getSecret();
        String account = loginProperties.getAccount();

        return new ClientSecretCacheKey(clientId, clientSecret, account);
    }

    /**
     * Verifies if the engine exists and sets its properties on the session properties. If the database is present, it will check if the database exists
     * and also sets its properties on the session
     * @param engineName
     * @return
     * @throws SQLException
     */
    private FireboltProperties getSessionPropertiesForNonSystemEngine(String engineName) throws SQLException {
        // set the engine name on session properties
        sessionProperties = sessionProperties.toBuilder().engine(engineName).build();

        Optional<ConnectionCache> connectionCacheOptional = !isConnectionCachingEnabled() ? Optional.empty() : Optional.of(connectionCache);

        Engine engine = fireboltEngineVersion2Service.getEngine(loginProperties, connectionCacheOptional, cacheService, getCacheKey());

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


    private FireboltProperties getSessionPropertiesForSystemEngine(String accessToken, String accountName) throws SQLException {
        String systemEngineEndpoint = getSystemEngineEndpointForAccount(accessToken, accountName);

        URL systemEngineUrl = UrlUtil.createUrl(systemEngineEndpoint);
        Map<String, String> systemEngineUrlUrlParams = UrlUtil.getQueryParameters(systemEngineUrl);
        for (Entry<String, String> e : systemEngineUrlUrlParams.entrySet()) {
            loginProperties.addProperty(e);
        }
        return loginProperties
                .toBuilder()
                .systemEngine(true)
                .compress(false)
                .host(systemEngineUrl.getHost())
                .build();
    }

    private String getSystemEngineEndpointForAccount(String accessToken, String accountName) throws SQLException {
        if (!isConnectionCachingEnabled()) {
            return fireboltGatewayUrlService.getUrl(accessToken, accountName);
        }

        // caching is enabled so check the cache first
        String systemEngineUrlFromCache = connectionCache.getSystemEngineUrl();
        if (StringUtils.isNotBlank(systemEngineUrlFromCache)) {
            return systemEngineUrlFromCache;
        }

        // not in cache so get it from the source.
        synchronized (connectionCache) {
            String systemEngineUrlFromFirebolt = fireboltGatewayUrlService.getUrl(accessToken, accountName);

            // only save it in cache if not empty
            if (StringUtils.isNotBlank(systemEngineUrlFromFirebolt)) {
                connectionCache.setSystemEngineUrl(systemEngineUrlFromFirebolt);
                cacheService.put(getCacheKey(), connectionCache);
            }

            return systemEngineUrlFromFirebolt;
        }

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

    @Override
    public int getInfraVersion() {
        return 2;
    }

}
