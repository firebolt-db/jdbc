package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.client.authentication.AuthenticationRequest;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.metadata.FireboltDatabaseMetadata;
import com.firebolt.jdbc.type.ParserVersion;
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
        return new FireboltDatabaseMetadata(httpConnectionUrl, this);
    }


    /**
     * For firebolt core the required parameters are:
     * - host - can be either an IPv4 address or a hostname
     * - port - must be a positive number between 1 and 65535
     */
    @Override
    protected void validateConnectionParameters() throws SQLException {
        Integer portInt = loginProperties.getPort();
        // check if the port is a valid port number
        if (portInt < 1 || portInt > 65535) {
            throw new SQLException("Port must be a positive number between 1 and 65535");
        }

        String host = loginProperties.getHost();
        if (StringUtils.isEmpty(host)) {
            throw new SQLException("Host is required for firebolt core");
        }

        // consider localhost a valid hostname
        if (!"localhost".equalsIgnoreCase(host)) {
            // Use Apache Commons Validator to validate IPv4, IPv6 and hostname
            DomainValidator domainValidator = DomainValidator.getInstance(false);
            InetAddressValidator inetAddressValidator = InetAddressValidator.getInstance();

            if (!inetAddressValidator.isValid(host) && !domainValidator.isValid(host)) {
                throw new SQLException("Invalid host: " + host);
            }
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
    protected boolean isConnectionCachingEnabled() {
        return false;
    }

    @Override
    public int getInfraVersion() {
        return 2;
    }
}