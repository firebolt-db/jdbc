package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.FireboltBackendType;
import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.client.authentication.AuthenticationRequest;
import com.firebolt.jdbc.client.authentication.DiscoveryAuthenticationRequest;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.client.discovery.FireboltDiscoveryClient;
import com.firebolt.jdbc.client.discovery.FireboltDiscoveryDocument;
import com.firebolt.jdbc.client.query.StatementClientImpl;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.metadata.FireboltDatabaseMetadata;
import com.firebolt.jdbc.metadata.FireboltSystemEngineDatabaseMetadata;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.type.ParserVersion;
import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import lombok.NonNull;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

public class FireboltDiscoveryConnection extends FireboltConnection {

    private static final String PROTOCOL_VERSION = "2.4";

    private final FireboltDiscoveryClient discoveryClient;
    private final String jdbcUrl;
    private final Properties connectionSettings;
    private String tokenEndpoint;
    private String accessToken;

    FireboltDiscoveryConnection(@NonNull Pair<String, Properties> urlConnectionSettings,
                                FireboltAuthenticationService fireboltAuthenticationService,
                                FireboltStatementService fireboltStatementService,
                                FireboltDiscoveryClient discoveryClient) throws SQLException {
        super(urlConnectionSettings.getKey(), urlConnectionSettings.getValue(), fireboltAuthenticationService,
                fireboltStatementService, PROTOCOL_VERSION, ParserVersion.CURRENT);
        this.jdbcUrl = urlConnectionSettings.getKey();
        this.connectionSettings = urlConnectionSettings.getValue();
        this.discoveryClient = discoveryClient;
        connect();
    }

    @ExcludeFromJacocoGeneratedReport
    FireboltDiscoveryConnection(@NonNull String url, Properties connectionSettings) throws SQLException {
        super(url, connectionSettings, PROTOCOL_VERSION, ParserVersion.CURRENT);
        this.jdbcUrl = url;
        this.connectionSettings = connectionSettings;
        OkHttpClient httpClient = getHttpClient(loginProperties);
        this.discoveryClient = new FireboltDiscoveryClient(httpClient);
        connect();
    }

    @Override
    protected void authenticate() throws SQLException {
        FireboltDiscoveryDocument discoveryDocument = discoveryClient.discover(loginProperties);
        tokenEndpoint = discoveryDocument.getTokenEndpoint();
        if (discoveryDocument.requiresAuthentication()) {
            validateAuthenticationParameters();
            accessToken = getAccessToken(loginProperties).orElse(null);
        } else {
            accessToken = loginProperties.getAccessToken();
        }
        sessionProperties = createSessionProperties(discoveryDocument);
        httpConnectionUrl = sessionProperties.getHttpConnectionUrl();
    }

    @Override
    protected void validateConnectionParameters() throws SQLException {
        if (StringUtils.isBlank(loginProperties.getHost())) {
            throw new FireboltException("Cannot connect: discovery host is missing");
        }
    }

    @Override
    protected boolean isConnectionCachingEnabled() {
        return false;
    }

    @Override
    public Optional<String> getConnectionUserAgentHeader() {
        return Optional.empty();
    }

    @Override
    public FireboltBackendType getBackendType() {
        return FireboltBackendType.DISCOVERY;
    }

    @Override
    public int getInfraVersion() {
        return 2;
    }

    @Override
    protected Optional<String> getAccessToken(FireboltProperties fireboltProperties) throws SQLException {
        if (StringUtils.isNotBlank(accessToken)) {
            return Optional.of(accessToken);
        }
        if (StringUtils.isNotBlank(fireboltProperties.getAccessToken())) {
            return Optional.of(fireboltProperties.getAccessToken());
        }
        if (StringUtils.isBlank(tokenEndpoint)) {
            return Optional.empty();
        }
        return super.getAccessToken(fireboltProperties);
    }

    @Override
    protected DatabaseMetaData retrieveMetaData() {
        if (!sessionProperties.isSystemEngine()) {
            return new FireboltDatabaseMetadata(httpConnectionUrl, this);
        }
        return new FireboltSystemEngineDatabaseMetadata(httpConnectionUrl, this);
    }

    @Override
    protected FireboltAuthenticationClient createFireboltAuthenticationClient(OkHttpClient httpClient) {
        return new FireboltAuthenticationClient(httpClient, this, loginProperties.getUserDrivers(), loginProperties.getUserClients()) {
            @Override
            public AuthenticationRequest getAuthenticationRequest(String username, String password, String host, String environment) {
                return new DiscoveryAuthenticationRequest(username, password, tokenEndpoint);
            }
        };
    }

    private FireboltProperties createSessionProperties(FireboltDiscoveryDocument discoveryDocument) {
        Properties discoveredProperties = new Properties();
        discoveryDocument.getParameters().forEach(discoveredProperties::setProperty);
        Optional.ofNullable(discoveryDocument.getQueryEndpoint()).ifPresent(endpoint -> addEndpointProperties(endpoint, discoveredProperties));
        FireboltProperties properties = new FireboltProperties(new Properties[] {discoveredProperties, UrlUtil.extractProperties(jdbcUrl), connectionSettings});
        if (StringUtils.isBlank(discoveryDocument.getQueryEndpoint())) {
            return properties;
        }
        return properties.toBuilder().systemEngine(properties.getEngine() == null).build();
    }

    private void validateAuthenticationParameters() throws SQLException {
        if (StringUtils.isBlank(loginProperties.getPrincipal())) {
            throw new FireboltException("Cannot connect: clientId is missing");
        }
        if (StringUtils.isBlank(loginProperties.getSecret())) {
            throw new FireboltException("Cannot connect: clientSecret is missing");
        }
    }

    private void addEndpointProperties(String endpoint, Properties properties) {
        URL endpointUrl = UrlUtil.createUrl(endpoint);
        properties.setProperty("host", endpointUrl.getHost());
        int port = endpointUrl.getPort() != -1 ? endpointUrl.getPort() : endpointUrl.getDefaultPort();
        if (port != -1) {
            properties.setProperty("port", String.valueOf(port));
        }
        properties.setProperty("ssl", String.valueOf("https".equalsIgnoreCase(endpointUrl.getProtocol())));
        Map<String, String> queryParameters = UrlUtil.getQueryParameters(endpointUrl);
        queryParameters.forEach(properties::setProperty);
    }
}
