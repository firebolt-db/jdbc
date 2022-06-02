package io.firebolt.jdbc.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.firebolt.jdbc.client.FireboltObjectMapper;
import io.firebolt.jdbc.client.HttpClientConfig;
import io.firebolt.jdbc.client.account.FireboltAccountClient;
import io.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import io.firebolt.jdbc.client.query.QueryClientImpl;
import io.firebolt.jdbc.metadata.FireboltDatabaseMetadata;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.service.FireboltAuthenticationService;
import io.firebolt.jdbc.service.FireboltEngineService;
import io.firebolt.jdbc.service.FireboltQueryService;
import io.firebolt.jdbc.statement.FireboltStatementImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Executor;

@Slf4j
public class FireboltConnectionImpl extends AbstractConnection {

  private final FireboltAuthenticationService fireboltAuthenticationService;
  private final FireboltEngineService fireboltEngineService;
  private final FireboltProperties loginProperties;
  boolean closed = true;
  private FireboltProperties sessionProperties;
  private final String httpConnectionUrl;

  public FireboltConnectionImpl(
      String url,
      Properties connectionSettings,
      FireboltAuthenticationService fireboltAuthenticationService,
      FireboltEngineService fireboltEngineService) {
    this.fireboltAuthenticationService = fireboltAuthenticationService;
    this.fireboltEngineService = fireboltEngineService;
    this.loginProperties = this.extractFireboltProperties(url, connectionSettings);
    this.httpConnectionUrl = getHttpConnectionUrl(loginProperties);
  }

  public FireboltConnectionImpl(String url, Properties connectionSettings) {
    ObjectMapper objectMapper = FireboltObjectMapper.getInstance();
    this.loginProperties = this.extractFireboltProperties(url, connectionSettings);
    this.httpConnectionUrl = getHttpConnectionUrl(loginProperties);
    CloseableHttpClient httpClient = getHttpClient(loginProperties);
    this.fireboltAuthenticationService =
        new FireboltAuthenticationService(
            new FireboltAuthenticationClient(httpClient, objectMapper));
    this.fireboltEngineService =
        new FireboltEngineService(new FireboltAccountClient(httpClient, objectMapper));
  }

  private static synchronized CloseableHttpClient getHttpClient(
      FireboltProperties fireboltProperties) {
    try {
      return HttpClientConfig.getInstance() == null
          ? HttpClientConfig.init(fireboltProperties)
          : HttpClientConfig.getInstance();
    } catch (CertificateException
        | NoSuchAlgorithmException
        | KeyStoreException
        | KeyManagementException
        | IOException e) {
      throw new RuntimeException("Could not instantiate http client", e);
    }
  }

  public synchronized FireboltConnectionImpl connect() throws FireboltException {
    if (closed) {
      try {
        FireboltConnectionTokens tokens =
            fireboltAuthenticationService.getConnectionTokens(
                httpConnectionUrl, loginProperties.getUser(), loginProperties.getPassword());
        String engineHost =
            fireboltEngineService.getEngineHost(
                httpConnectionUrl,
                loginProperties.getDatabase(),
                loginProperties.getEngine(),
                loginProperties.getAccount(),
                tokens.getAccessToken());

        this.sessionProperties = loginProperties.toBuilder().host(engineHost).build();
        closed = false;
      } catch (Exception e) {
        throw new FireboltException("Could not connect", e);
      }
    }
    return this;
  }

  public FireboltProperties getSessionProperties() {
    return this.sessionProperties;
  }

  @Override
  public Statement createStatement() throws SQLException {
    FireboltConnectionTokens connectionTokens =
        fireboltAuthenticationService.getConnectionTokens(
            httpConnectionUrl, sessionProperties.getUser(), sessionProperties.getPassword());
    return FireboltStatementImpl.builder()
        .fireboltQueryService(
            new FireboltQueryService(new QueryClientImpl(HttpClientConfig.getInstance())))
        .sessionProperties(sessionProperties)
        .connectionTokens(connectionTokens)
        .build();
  }

  public Statement createStatementWithTemporaryProperties(FireboltProperties tmpProperties) {
    FireboltConnectionTokens connectionTokens =
            fireboltAuthenticationService.getConnectionTokens(
                    httpConnectionUrl, tmpProperties.getUser(), tmpProperties.getPassword());
    return FireboltStatementImpl.builder()
            .fireboltQueryService(
                    new FireboltQueryService(new QueryClientImpl(HttpClientConfig.getInstance())))
            .sessionProperties(tmpProperties)
            .connectionTokens(connectionTokens)
            .build();
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    return false;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return this.closed;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new FireboltDatabaseMetadata(this.httpConnectionUrl, this);
  }

  @Override
  public String getCatalog() throws SQLException {
    return sessionProperties.getDatabase();
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public String getSchema() throws SQLException {
    return sessionProperties.getDatabase();
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    sessionProperties = sessionProperties.toBuilder().database(schema).build();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    this.close();
  }

  private FireboltProperties extractFireboltProperties(
      String jdbcUri, Properties connectionProperties) {
    Properties propertiesFromUrl = FireboltJdbcUrlUtil.extractProperties(jdbcUri);
    return FireboltProperties.of(propertiesFromUrl, connectionProperties);
  }

  private String getHttpConnectionUrl(FireboltProperties newSessionProperties) {
    return Boolean.TRUE.equals(newSessionProperties.getSsl())
        ? "https://" + newSessionProperties.getHost()
        : "htto://" + newSessionProperties.getHost();
  }

  @Override
  public void setCatalog(String db) throws SQLException {
    // no-op
  }
}
