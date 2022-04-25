package io.firebolt.jdbc.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.firebolt.jdbc.client.FireboltObjectMapper;
import io.firebolt.jdbc.client.HttpClientConfig;
import io.firebolt.jdbc.client.account.FireboltAccountClient;
import io.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.connection.settings.PropertiesToFireboltPropertiesTransformer;
import io.firebolt.jdbc.connection.settings.PropertyUtil;
import io.firebolt.jdbc.service.FireboltAuthenticationService;
import io.firebolt.jdbc.service.FireboltEngineService;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.sql.*;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

@Slf4j
public class FireboltConnectionImpl implements Connection {

  private final FireboltAuthenticationService fireboltAuthenticationService;
  private final FireboltEngineService fireboltEngineService;
  boolean closed = true;
  private FireboltProperties fireboltProperties;

  public FireboltConnectionImpl(
      String url,
      Properties connectionSettings,
      FireboltAuthenticationService fireboltAuthenticationService,
      FireboltEngineService fireboltEngineService) {
    this.fireboltAuthenticationService = fireboltAuthenticationService;
    this.fireboltEngineService = fireboltEngineService;
    this.initConnection(url, connectionSettings, null);
    closed = false;
  }

  public FireboltConnectionImpl(String url, Properties connectionSettings) {
    ObjectMapper objectMapper = FireboltObjectMapper.getInstance();
    this.fireboltProperties =
        PropertyUtil.extractFireboltProperties(
            url, connectionSettings, new PropertiesToFireboltPropertiesTransformer());
    CloseableHttpClient httpClient = getHttpClient(fireboltProperties);
    fireboltAuthenticationService =
        new FireboltAuthenticationService(
            new FireboltAuthenticationClient(httpClient, objectMapper));
    fireboltEngineService =
        new FireboltEngineService(new FireboltAccountClient(httpClient, objectMapper));
    this.initConnection(url, connectionSettings, fireboltProperties);
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

  private void initConnection(
      String url, Properties connectionSettings, FireboltProperties fireboltProperties) {
    fireboltProperties =
        fireboltProperties == null
            ? PropertyUtil.extractFireboltProperties(
                url, connectionSettings, new PropertiesToFireboltPropertiesTransformer())
            : fireboltProperties;
    log.info("Connecting to {}", url);
    try {
      FireboltConnectionTokens tokens =
          fireboltAuthenticationService.getConnectionTokens(
              fireboltProperties.getHost(),
              fireboltProperties.getUser(),
              fireboltProperties.getPassword());
      String engineAddress =
          fireboltEngineService.getEngineAddress(
              fireboltProperties.getHost(),
              fireboltProperties.getDatabase(),
              fireboltProperties.getEngine(),
              fireboltProperties.getAccount(),
              tokens.getAccessToken());
      log.info("Engine address: {}", engineAddress);
    } catch (Exception e) {
      throw new RuntimeException("Could not connect", e);
    }
  }

  @Override
  public void close() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Statement createStatement() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void commit() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void rollback() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isClosed() throws SQLException {
    return this.closed;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return false;
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {}

  @Override
  public String getCatalog() throws SQLException {
    return fireboltProperties.getDatabase();
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {}

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    return Collections.emptyMap();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {}

  @Override
  public int getHoldability() throws SQLException {
    return 0;
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {}

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {}

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public Clob createClob() throws SQLException {
    return null;
  }

  @Override
  public Blob createBlob() throws SQLException {
    return null;
  }

  @Override
  public NClob createNClob() throws SQLException {
    return null;
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    return null;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {}

  @Override
  public String getClientInfo(String name) throws SQLException {
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    return null;
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {}

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    return null;
  }

  @Override
  public String getSchema() throws SQLException {
    return fireboltProperties.getDatabase();
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    fireboltProperties = fireboltProperties.toBuilder().database(schema).build();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    this.close();
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return 0;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isAssignableFrom(getClass())) {
      return iface.cast(this);
    }
    throw new SQLException("Cannot unwrap to " + iface.getName());
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface.isAssignableFrom(getClass());
  }
}
