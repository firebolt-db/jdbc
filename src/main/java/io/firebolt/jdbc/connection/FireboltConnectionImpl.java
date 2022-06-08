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
import org.apache.commons.lang3.StringUtils;
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
public class FireboltConnectionImpl extends AbstractConnection {

  private final FireboltAuthenticationService fireboltAuthenticationService;
  private final FireboltEngineService fireboltEngineService;
  private final FireboltQueryService fireboltQueryService;
  private final FireboltProperties loginProperties;
  private boolean closed = true;
  private FireboltProperties sessionProperties;
  private final String httpConnectionUrl;

  private final List<Statement> statements;

  private static final String LOCALHOST = "localhost";

  public FireboltConnectionImpl(
      String url,
      Properties connectionSettings,
      FireboltAuthenticationService fireboltAuthenticationService,
      FireboltEngineService fireboltEngineService,
      FireboltQueryService fireboltQueryService)
      throws FireboltException {
    this.fireboltAuthenticationService = fireboltAuthenticationService;
    this.fireboltEngineService = fireboltEngineService;
    this.loginProperties = this.extractFireboltProperties(url, connectionSettings);
    this.httpConnectionUrl = getHttpConnectionUrl(loginProperties);
    this.fireboltQueryService = fireboltQueryService;
    this.statements = new ArrayList<>();
    this.connect();
  }

  public FireboltConnectionImpl(String url, Properties connectionSettings)
      throws FireboltException {
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
    this.connect();
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

  private FireboltConnectionImpl connect() throws FireboltException {
    try {
      if (!StringUtils.equalsIgnoreCase(LOCALHOST, loginProperties.getHost())) {
        Optional<FireboltConnectionTokens> fireboltConnectionTokens = this.getConnectionTokens();
          String engineHost =
              fireboltEngineService.getEngineHost(
                  httpConnectionUrl,
                  loginProperties.getDatabase(),
                  loginProperties.getEngine(),
                  loginProperties.getAccount(), fireboltConnectionTokens.map(FireboltConnectionTokens::getAccessToken).orElse(null));
          this.sessionProperties = loginProperties.toBuilder().host(engineHost).build();
        } else {
          this.sessionProperties = loginProperties;
        }
      closed = false;
      log.debug("Connection opened");
    } catch (Exception e) {
      throw new FireboltException("Could not connect", e);
    }
    return this;
  }

  private Optional<FireboltConnectionTokens> getConnectionTokens() {
    if (!StringUtils.equalsIgnoreCase(LOCALHOST, loginProperties.getHost())) {
      return Optional.of(
          fireboltAuthenticationService.getConnectionTokens(
              httpConnectionUrl, loginProperties.getUser(), loginProperties.getPassword()));
    }
    return Optional.empty();
  }

  public FireboltProperties getSessionProperties() {
    return this.sessionProperties;
  }

  @Override
  public Statement createStatement() throws SQLException {
    checkConnectionIsNotClose();
    Optional<FireboltConnectionTokens> connectionTokens = this.getConnectionTokens();
    FireboltStatementImpl fireboltStatement =
        FireboltStatementImpl.builder()
            .fireboltQueryService(fireboltQueryService)
            .sessionProperties(sessionProperties)
            .accessToken(connectionTokens.map(FireboltConnectionTokens::getAccessToken).orElse(null))
            .build();
    this.statements.add(fireboltStatement);
    return fireboltStatement;
  }

  private void addStatement(Statement statement) throws SQLException {
    synchronized (this) {
      checkConnectionIsNotClose();
      this.statements.add(statement);
    }
  }

  public Statement createStatementWithTemporaryProperties(FireboltProperties tmpProperties)
      throws SQLException {
    checkConnectionIsNotClose();
    Optional<FireboltConnectionTokens> connectionTokens = this.getConnectionTokens();
    FireboltStatementImpl fireboltStatement =
        FireboltStatementImpl.builder()
            .fireboltQueryService(fireboltQueryService)
            .sessionProperties(tmpProperties)
            .accessToken(connectionTokens.map(FireboltConnectionTokens::getAccessToken).orElse(null))
            .build();
    this.addStatement(fireboltStatement);
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
    log.debug("Aborting connection");
    this.close();
  }

  @Override
  public void close() throws SQLException {
    log.debug("Closing connection");
    synchronized (this) {
      if (!this.isClosed()) {
        for (Statement statement : this.statements) {
          try {
            statement.close();
          } catch (Exception e) {
            log.warn("Could not close statement", e);
          }
        }
        this.statements.clear();
        closed = true;
      }
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
    return Boolean.TRUE.equals(newSessionProperties.getSsl())
        ? "https://" + hostAndPort
        : "http://" + hostAndPort;
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
    log.warn("Could not call %s", new Throwable().getStackTrace()[0].getMethodName());
    return null;
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
    Optional<FireboltConnectionTokens> connectionTokens = this.getConnectionTokens();
    PreparedStatement statement =
        FireboltPreparedStatement.statementBuilder()
            .fireboltQueryService(fireboltQueryService)
            .sessionProperties(this.getSessionProperties())
            .accessToken(connectionTokens.map(FireboltConnectionTokens::getAccessToken).orElse(null))
            .sql(sql)
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

  @Override
  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw new SQLException("Timeout value cannot be less 0");
    }
    if (isClosed()) {
      return false;
    }
    try (Statement s = createStatement()) {
      s.execute("SELECT 1");
      return true;
    } catch (Exception e) {
      log.warn("Connection is not valid", e);
      return false;
    }
  }

  private void checkConnectionIsNotClose() throws SQLException {
    if (isClosed()) {
      throw new SQLException("Cannot proceed: connection closed");
    }
  }
}
