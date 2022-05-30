package io.firebolt.jdbc.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.firebolt.jdbc.client.FireboltObjectMapper;
import io.firebolt.jdbc.client.HttpClientConfig;
import io.firebolt.jdbc.client.account.FireboltAccountClient;
import io.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import io.firebolt.jdbc.client.query.QueryClientImpl;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.metadata.FireboltDatabaseMetadata;
import io.firebolt.jdbc.preparedstatement.FireboltPreparedStatement;
import io.firebolt.jdbc.service.FireboltAuthenticationService;
import io.firebolt.jdbc.service.FireboltEngineService;
import io.firebolt.jdbc.service.FireboltQueryService;
import io.firebolt.jdbc.statement.FireboltStatementImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;

@Slf4j
public class FireboltConnectionImpl extends AbstractConnection {

  private final FireboltAuthenticationService fireboltAuthenticationService;
  private final FireboltEngineService fireboltEngineService;
  private final FireboltQueryService fireboltQueryService;
  private final FireboltProperties loginProperties;
  private boolean closed = true;
  private FireboltProperties sessionProperties;
  private final String httpConnectionUrl;

  private final List<Statement> statements;

  public FireboltConnectionImpl(
      String url,
      Properties connectionSettings,
      FireboltAuthenticationService fireboltAuthenticationService,
      FireboltEngineService fireboltEngineService,
      FireboltQueryService fireboltQueryService) {
    this.fireboltAuthenticationService = fireboltAuthenticationService;
    this.fireboltEngineService = fireboltEngineService;
    this.loginProperties = this.extractFireboltProperties(url, connectionSettings);
    this.httpConnectionUrl = getHttpConnectionUrl(loginProperties);
    this.fireboltQueryService = fireboltQueryService;
    this.statements = new ArrayList<>();
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
    this.fireboltQueryService = new FireboltQueryService(new QueryClientImpl(httpClient));
    this.statements = new ArrayList<>();
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
    checkConnectionIsNotClose();
    FireboltConnectionTokens connectionTokens =
        fireboltAuthenticationService.getConnectionTokens(
            httpConnectionUrl, sessionProperties.getUser(), sessionProperties.getPassword());
    FireboltStatementImpl fireboltStatement =
        FireboltStatementImpl.builder()
            .fireboltQueryService(fireboltQueryService)
            .sessionProperties(sessionProperties)
            .connectionTokens(connectionTokens)
            .build();
    this.statements.add(fireboltStatement);
    return fireboltStatement;
  }

  public Statement createStatementWithTemporaryProperties(FireboltProperties tmpProperties)
      throws SQLException {
    checkConnectionIsNotClose();
    FireboltConnectionTokens connectionTokens =
        fireboltAuthenticationService.getConnectionTokens(
            httpConnectionUrl, tmpProperties.getUser(), tmpProperties.getPassword());
    FireboltStatementImpl fireboltStatement =
        FireboltStatementImpl.builder()
            .fireboltQueryService(fireboltQueryService)
            .sessionProperties(tmpProperties)
            .connectionTokens(connectionTokens)
            .build();
    this.statements.add(fireboltStatement);
    return fireboltStatement;
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

  @Override
  public void close() throws SQLException {
    for (Statement statement : this.statements) {
      try {
        statement.close();
      } catch (Exception e) {
        log.warn("Could not close statement", e);
      }
    }
    this.closed = true;
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
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    checkConnectionIsNotClose();
    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
      throw new SQLFeatureNotSupportedException();
    }
    return createPreparedStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    checkConnectionIsNotClose();
    return this.createPreparedStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    checkConnectionIsNotClose();
    return this.prepareStatement(sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    checkConnectionIsNotClose();
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    checkConnectionIsNotClose();
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    checkConnectionIsNotClose();
    throw new SQLFeatureNotSupportedException();
  }

  private PreparedStatement createPreparedStatement(String sql) throws SQLException {
    checkConnectionIsNotClose();
    FireboltConnectionTokens connectionTokens =
        fireboltAuthenticationService.getConnectionTokens(
            httpConnectionUrl, sessionProperties.getUser(), sessionProperties.getPassword());
    PreparedStatement statement =
        FireboltPreparedStatement.statementBuilder()
            .fireboltQueryService(fireboltQueryService)
            .sessionProperties(this.getSessionProperties())
            .connectionTokens(connectionTokens)
            .sql(sql)
            .build();
    this.statements.add(statement);
    return statement;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    // no-op
  }

  private void checkConnectionIsNotClose() throws SQLException {
    if (isClosed()) {
      throw new SQLException("Cannot proceed: connection closed");
    }
  }
}
