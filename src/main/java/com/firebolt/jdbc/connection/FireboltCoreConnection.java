package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.client.authentication.AuthenticationRequest;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.metadata.FireboltDatabaseMetadata;
import com.firebolt.jdbc.type.ParserVersion;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
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
    }

    protected DatabaseMetaData retrieveMetaData() {
        return new FireboltDatabaseMetadata(getEndpoint(), this);
    }


    /**
     * For firebolt core the required parameters are:
     * - url - in the form of: <protocol>://<host>:<port>
     */
    @Override
    protected void validateConnectionParameters() throws SQLException {
        if (StringUtils.isEmpty(loginProperties.getUrl())) {
            throw new SQLException("Url is required for firebolt core");
        }

        try {
            URL fireboltCoreUrl = new URL(loginProperties.getUrl());
            String protocol = fireboltCoreUrl.getProtocol();
            String host = fireboltCoreUrl.getHost();
            int port = fireboltCoreUrl.getPort();

            if (StringUtils.isEmpty(protocol) || StringUtils.isEmpty(host)) {
                throw new SQLException("Invalid URL format. URL must be in the form: <protocol>://<host>:<port>");
            }

            // Validate port range
            if (port <= 0 || port > 65535) {
                throw new SQLException("Invalid URL format. URL must be in the form: <protocol>://<host>:<port>");
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
                    throw new SQLException("Invalid URL format. URL must be in the form: <protocol>://<host>:<port>");
                }
            }
        } catch (MalformedURLException e) {
            throw new SQLException("Invalid URL format. URL must be in the form: <protocol>://<host>:<port>");
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

    @Override
    public String getEndpoint() {
        return loginProperties.getUrl();
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
    protected boolean isConnectionCachingEnabled() {
        return false;
    }

    @Override
    public int getInfraVersion() {
        return 2;
    }
}