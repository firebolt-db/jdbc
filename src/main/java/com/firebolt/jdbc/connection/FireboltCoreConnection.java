package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.FireboltBackendType;
import com.firebolt.jdbc.client.authentication.AuthenticationRequest;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.metadata.FireboltDatabaseMetadata;
import com.firebolt.jdbc.type.ParserVersion;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;
import lombok.CustomLog;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.DomainValidator;
import org.apache.commons.validator.routines.InetAddressValidator;

@CustomLog
public class FireboltCoreConnection extends FireboltConnection {

    private static final String PROTOCOL_VERSION = "2.3";

    public FireboltCoreConnection(String url, Properties connectionSettings) throws SQLException {
        super(url, connectionSettings, PROTOCOL_VERSION, ParserVersion.CURRENT);
        connect();
    }

    @Override
    protected void authenticate() throws SQLException {
        // there is no authentication for firebolt core

        // When running firebolt core the login properties are the session properties
        sessionProperties = loginProperties;

        // StatementClientImpl expects the URI creation to use host/port and isSsl. So set these values from the URL
        try {
            URI uri = new URI(sessionProperties.getUrl());

            sessionProperties = sessionProperties.toBuilder()
                    .host(uri.getHost())
                    .port(uri.getPort())
                    .ssl(uri.getScheme().startsWith("https"))
                    .build();

            // the constructor sets the httpConnectionUrl, but it checks the host and port. For Core the httpConnectionUrl should be the url parameter.
            this.httpConnectionUrl = sessionProperties.getUrl();

            // need to validate if the database exists after we set the session properties since we need to make a call to the backend
            validateDatabaseIfNeeded();

        } catch (URISyntaxException e) {
            // this should not happen as we had validate the url already validated
            throw new SQLException("Invalid url: " + sessionProperties.getUrl());
        }
    }

    @Override
    protected DatabaseMetaData retrieveMetaData() {
        return new FireboltDatabaseMetadata(getEndpoint(), this);
    }

    /**
     * If database is passed in, then make sure that the database exists, by calling a use database
     */
    private void validateDatabaseIfNeeded() throws SQLException {
        String database = loginProperties.getDatabase();
        if (StringUtils.isNotBlank(database)) {
            log.debug("Validating the database {} exists.", database);

            try (Statement statement = this.createStatement()) {
                statement.executeUpdate(String.format("USE DATABASE \"%s\"", database));
            }
        }
    }

    /**
     * For firebolt core the required parameters are:
     * - url - in the form of: <protocol>://<host>:<port>
     */
    @Override
    protected void validateConnectionParameters() throws SQLException {
        String url = loginProperties.getUrl();
        if (StringUtils.isBlank(url)) {
            throw new SQLException("Url is required for firebolt core");
        }

        try {
            if (!url.startsWith("http://") && !url.startsWith("https://")) {
                throw new SQLException("Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. You did not pass in the protocol. It has to be either http or https.");
            }

            URL fireboltCoreUrl = new URL(url);
            String host = fireboltCoreUrl.getHost();
            int port = fireboltCoreUrl.getPort();

            if (StringUtils.isEmpty(host)) {
                throw new SQLException("Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. You did not pass in the protocol or the host");
            }

            // Validate port range
            if (port == -1) {
                throw new SQLException("Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. You must specify the port.");
            }
            if (port <= 0 || port > 65535) {
                throw new SQLException("Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. The port value should be a positive integer between 1 and 65535. You have the port as:" + port);
            }

            // Validate hostname
            if (!"localhost".equalsIgnoreCase(host)) {
                // Remove IPv6 brackets if present
                String hostToValidate = host;
                boolean isIPv6 = host.startsWith("[") && host.endsWith("]");
                if (isIPv6) {
                    hostToValidate = host.substring(1, host.length() - 1);
                }

                InetAddressValidator inetAddressValidator = InetAddressValidator.getInstance();
                DomainValidator domainValidator = DomainValidator.getInstance(false);

                if (!inetAddressValidator.isValid(hostToValidate) && !domainValidator.isValid(hostToValidate)) {
                    throw new SQLException("Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. The host is not valid. It must be a valid IPv4, IPv6 or domain name or even localhost");
                }
            }
        } catch (MalformedURLException e) {
            throw new SQLException("Invalid URL format. URL must be in the form: <protocol>://<host>:<port>. "+e.getMessage());
        }
    }

    @Override
    protected FireboltAuthenticationClient createFireboltAuthenticationClient(OkHttpClient httpClient) {
        return new FireboltAuthenticationClient(httpClient, this, loginProperties.getUserDrivers(), loginProperties.getUserClients()) {
            @Override
            protected AuthenticationRequest getAuthenticationRequest(String username, String password, String host, String environment) {
                return null;
            }
        };
    }

    /**
     * There is no authentication for core, thus no token
     * @return
     * @throws SQLException
     */
    @Override
    public Optional<String> getAccessToken() throws SQLException {
        return Optional.empty();
    }

    @Override
    public Optional<String> getConnectionUserAgentHeader() {
        return Optional.empty();
    }

    @Override
    public FireboltBackendType getBackendType() {
        return FireboltBackendType.FIREBOLT_CORE;
    }

    @Override
    protected boolean isConnectionCachingEnabled() {
        return false;
    }

    @Override
    public int getInfraVersion() {
        return 2;
    }
}