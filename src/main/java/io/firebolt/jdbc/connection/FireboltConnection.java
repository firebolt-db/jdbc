package io.firebolt.jdbc.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.firebolt.jdbc.client.FireboltObjectMapper;
import io.firebolt.jdbc.client.HttpClientConfig;
import io.firebolt.jdbc.client.account.FireboltAccountClient;
import io.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import io.firebolt.jdbc.client.query.StatementClientImpl;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.ExceptionType;
import io.firebolt.jdbc.exception.FireboltException;
import io.firebolt.jdbc.metadata.FireboltDatabaseMetadata;
import io.firebolt.jdbc.preparedstatement.FireboltPreparedStatement;
import io.firebolt.jdbc.service.FireboltAuthenticationService;
import io.firebolt.jdbc.service.FireboltEngineService;
import io.firebolt.jdbc.service.FireboltStatementService;
import io.firebolt.jdbc.statement.FireboltStatement;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executor;

@Slf4j
public class FireboltConnection extends AbstractConnection {

  private final FireboltAuthenticationService fireboltAuthenticationService;
  private final FireboltEngineService fireboltEngineService;
  private final FireboltStatementService fireboltStatementService;
  private final FireboltProperties loginProperties;
  private boolean closed = true;
  private FireboltProperties sessionProperties;
  private final String httpConnectionUrl;

  private final List<FireboltStatement> statements;

  private static final String LOCALHOST = "localhost";

  public FireboltConnection(
      @NonNull String url,
      Properties connectionSettings,
      FireboltAuthenticationService fireboltAuthenticationService,
      FireboltEngineService fireboltEngineService,
      FireboltStatementService fireboltStatementService)
      throws FireboltException {
    this.fireboltAuthenticationService = fireboltAuthenticationService;
    this.fireboltEngineService = fireboltEngineService;
    this.loginProperties = this.extractFireboltProperties(url, connectionSettings);
    loginProperties.getAdditionalProperties().remove("connector_versions");
    this.httpConnectionUrl = getHttpConnectionUrl(loginProperties);
    this.fireboltStatementService = fireboltStatementService;
    this.statements = new ArrayList<>();
    this.connect();
  }

  public FireboltConnection(String url, Properties connectionSettings) throws FireboltException {
    ObjectMapper objectMapper = FireboltObjectMapper.getInstance();
    this.loginProperties = this.extractFireboltProperties(url, connectionSettings);
    String driverVersions = loginProperties.getAdditionalProperties().remove("driver_versions");
    String clientVersions = loginProperties.getAdditionalProperties().remove("client_versions");
    this.httpConnectionUrl = getHttpConnectionUrl(loginProperties);
    CloseableHttpClient httpClient = getHttpClient(loginProperties);
    this.fireboltAuthenticationService =
        new FireboltAuthenticationService(
            new FireboltAuthenticationClient(
                httpClient, objectMapper, this, driverVersions, clientVersions));
    this.fireboltEngineService =
        new FireboltEngineService(
            new FireboltAccountClient(
                httpClient, objectMapper, this, driverVersions, clientVersions));
    this.fireboltStatementService =
        new FireboltStatementService(
            new StatementClientImpl(httpClient, this, driverVersions, clientVersions));
    this.statements = new ArrayList<>();
    this.connect();
  }

  private static synchronized CloseableHttpClient getHttpClient(
      FireboltProperties fireboltProperties) throws FireboltException {
    try {
      return HttpClientConfig.getInstance() == null
          ? HttpClientConfig.init(fireboltProperties)
          : HttpClientConfig.getInstance();
    } catch (CertificateException
        | NoSuchAlgorithmException
        | KeyStoreException
        | KeyManagementException
        | IOException e) {
      throw new FireboltException("Could not instantiate http client", e);
    }
  }

  private void connect() throws FireboltException {
    try {
      if (!StringUtils.equalsIgnoreCase(LOCALHOST, loginProperties.getHost())) {
        String engineHost = fireboltEngineService.getEngineHost(httpConnectionUrl, loginProperties);
        this.sessionProperties = loginProperties.toBuilder().host(engineHost).build();
      } else {
        this.sessionProperties = loginProperties;
      }
      closed = false;
      log.debug("Connection opened");
    } catch (FireboltException ex) {
      if (ex.getType() == ExceptionType.EXPIRED_TOKEN) {
        log.debug("Refreshing expired-token to establish new connection");
        fireboltAuthenticationService.removeConnectionTokens(httpConnectionUrl, loginProperties);
        this.connect();
      } else {
        throw ex;
      }
    }
  }

  public void removeExpiredTokens() throws FireboltException {
    fireboltAuthenticationService.removeConnectionTokens(httpConnectionUrl, loginProperties);
  }

  public Optional<FireboltConnectionTokens> getConnectionTokens() throws FireboltException {
    if (!StringUtils.equalsIgnoreCase(LOCALHOST, loginProperties.getHost())) {
      return Optional.of(
          fireboltAuthenticationService.getConnectionTokens(httpConnectionUrl, loginProperties));
    }
    return Optional.empty();
  }

  public FireboltProperties getSessionProperties() {
    return this.sessionProperties;
  }

  @Override
  public Statement createStatement() throws SQLException {
    return this.createStatement(this.getSessionProperties());
  }

  public Statement createStatement(FireboltProperties tmpProperties) throws SQLException {
    checkConnectionIsNotClose();
    FireboltStatement fireboltStatement =
        FireboltStatement.builder()
            .statementService(fireboltStatementService)
            .sessionProperties(tmpProperties)
            .connection(this)
            .build();
    this.addStatement(fireboltStatement);
    return fireboltStatement;
  }

  private void addStatement(FireboltStatement statement) throws SQLException {
    synchronized (statements) {
      checkConnectionIsNotClose();
      this.statements.add(statement);
    }
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
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
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
    log.debug("Aborting connection");
    this.close();
  }

  @Override
  public void close() throws SQLException {
    log.debug("Closing connection");
    synchronized (statements) {
      if (!this.isClosed()) {
        for (FireboltStatement statement : this.statements) {
          try {
            statement.close(false);
          } catch (Exception e) {
            log.warn("Could not close statement", e);
          }
        }
      }
      statements.clear();
      closed = true;
    }
    log.debug("Connection closed");
  }

  private FireboltProperties extractFireboltProperties(
      String jdbcUri, Properties connectionProperties) {
    Properties propertiesFromUrl = FireboltJdbcUrlUtil.extractProperties(jdbcUri);
    return FireboltProperties.of(propertiesFromUrl, connectionProperties);
  }

  private String getHttpConnectionUrl(FireboltProperties newSessionProperties) {
    String hostAndPort = newSessionProperties.getHost() + ":" + newSessionProperties.getPort();
    return newSessionProperties.isSsl() ? "https://" + hostAndPort : "http://" + hostAndPort;
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
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLFeatureNotSupportedException();
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
    FireboltPreparedStatement statement =
        FireboltPreparedStatement.statementBuilder()
            .statementService(fireboltStatementService)
            .sessionProperties(this.getSessionProperties())
            .sql(sql)
            .connection(this)
            .build();
    this.addStatement(statement);
    return statement;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    // no-op
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE
        || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY
        || resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new SQLFeatureNotSupportedException();
    }
    return createStatement();
  }

  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw new SQLException("Timeout value cannot be less than 0");
    }
    if (isClosed()) {
      return false;
    }
    try {
      validateConnection(this.getSessionProperties());
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public void validateConnection(FireboltProperties fireboltProperties) throws SQLException {
    try (Statement s = createStatement(fireboltProperties)) {
      s.execute("SELECT 1");
    } catch (Exception e) {
      log.warn("Connection is not valid", e);
      throw e;
    }
  }

  private void checkConnectionIsNotClose() throws SQLException {
    if (isClosed()) {
      throw new FireboltException("Cannot proceed: connection closed");
    }
  }

  public void removeClosedStatement(FireboltStatement fireboltStatement) {
    synchronized (statements) {
      this.statements.remove(fireboltStatement);
    }
  }

  public synchronized void addProperty(Pair<String, String> property) throws FireboltException {
    try {
      FireboltProperties tmpProperties = FireboltProperties.copy(this.sessionProperties);
      tmpProperties.addProperty(property);
      validateConnection(tmpProperties);
      this.sessionProperties = tmpProperties;
    } catch (FireboltException e) {
      throw e;
    } catch (Exception e) {
      throw new FireboltException(
          String.format("Could not set property %s=%s", property.getLeft(), property.getRight()),
          e);
    }
  }

  @Override
  public void commit() throws SQLException {
    //no-op
  }

  @Override
  public void rollback() throws SQLException {
    //no-op
  }
}
