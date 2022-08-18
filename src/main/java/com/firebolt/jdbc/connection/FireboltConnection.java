package com.firebolt.jdbc.connection;

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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.PropertyUtil;
import com.firebolt.jdbc.client.FireboltObjectMapper;
import com.firebolt.jdbc.client.HttpClientConfig;
import com.firebolt.jdbc.client.account.FireboltAccountClient;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.client.query.StatementClientImpl;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.metadata.FireboltDatabaseMetadata;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.statement.preparedstatement.FireboltPreparedStatement;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FireboltConnection extends AbstractConnection {

	private final FireboltAuthenticationService fireboltAuthenticationService;
	private final FireboltEngineService fireboltEngineService;
	private final FireboltStatementService fireboltStatementService;
	private final FireboltProperties loginProperties;
	private final String httpConnectionUrl;
	private final List<FireboltStatement> statements;
	private final int connectionTimeout;
	private boolean closed = true;
	private FireboltProperties sessionProperties;
	private int networkTimeout;

	public FireboltConnection(@NonNull String url, Properties connectionSettings,
			FireboltAuthenticationService fireboltAuthenticationService, FireboltEngineService fireboltEngineService,
			FireboltStatementService fireboltStatementService) throws FireboltException {
		this.fireboltAuthenticationService = fireboltAuthenticationService;
		this.fireboltEngineService = fireboltEngineService;
		this.loginProperties = this.extractFireboltProperties(url, connectionSettings);
		loginProperties.getAdditionalProperties().remove("user_clients");
		loginProperties.getAdditionalProperties().remove("user_drivers");
		this.httpConnectionUrl = getHttpConnectionUrl(loginProperties);
		this.fireboltStatementService = fireboltStatementService;
		this.statements = new ArrayList<>();
		this.connectionTimeout = loginProperties.getConnectionTimeoutMillis();
		this.networkTimeout = loginProperties.getSocketTimeoutMillis();
		this.connect();
	}

	public FireboltConnection(@NonNull String url, Properties connectionSettings) throws FireboltException {
		ObjectMapper objectMapper = FireboltObjectMapper.getInstance();
		this.loginProperties = this.extractFireboltProperties(url, connectionSettings);
		String driverVersions = loginProperties.getAdditionalProperties().remove("user_drivers");
		String clientVersions = loginProperties.getAdditionalProperties().remove("user_clients");
		this.httpConnectionUrl = getHttpConnectionUrl(loginProperties);
		CloseableHttpClient httpClient = getHttpClient(loginProperties);
		this.fireboltAuthenticationService = new FireboltAuthenticationService(
				new FireboltAuthenticationClient(httpClient, objectMapper, this, driverVersions, clientVersions));
		this.fireboltEngineService = new FireboltEngineService(
				new FireboltAccountClient(httpClient, objectMapper, this, driverVersions, clientVersions));
		this.fireboltStatementService = new FireboltStatementService(
				new StatementClientImpl(httpClient, this, objectMapper, driverVersions, clientVersions));
		this.statements = new ArrayList<>();
		this.connectionTimeout = loginProperties.getConnectionTimeoutMillis();
		this.networkTimeout = loginProperties.getSocketTimeoutMillis();
		this.connect();
	}

	private static synchronized CloseableHttpClient getHttpClient(FireboltProperties fireboltProperties)
			throws FireboltException {
		try {
			return HttpClientConfig.getInstance() == null ? HttpClientConfig.init(fireboltProperties)
					: HttpClientConfig.getInstance();
		} catch (CertificateException | NoSuchAlgorithmException | KeyStoreException | KeyManagementException
				| IOException e) {
			throw new FireboltException("Could not instantiate http client", e);
		}
	}

	private void connect() throws FireboltException {
		String accessToken = this.getConnectionTokens().map(FireboltConnectionTokens::getAccessToken).orElse("");
		if (!PropertyUtil.isLocalDb(this.loginProperties)) {
			String engineHost = fireboltEngineService.getEngineHost(httpConnectionUrl, loginProperties, accessToken);
			this.sessionProperties = loginProperties.toBuilder().host(engineHost).build();
		} else {
			this.sessionProperties = loginProperties;
		}
		closed = false;
		log.debug("Connection opened");
	}

	public void removeExpiredTokens() throws FireboltException {
		fireboltAuthenticationService.removeConnectionTokens(httpConnectionUrl, loginProperties);
	}

	public Optional<FireboltConnectionTokens> getConnectionTokens() throws FireboltException {
		if (!PropertyUtil.isLocalDb(loginProperties)) {
			return Optional.of(fireboltAuthenticationService.getConnectionTokens(httpConnectionUrl, loginProperties));
		}
		return Optional.empty();
	}

	public FireboltProperties getSessionProperties() {
		return this.sessionProperties;
	}

	@Override
	public Statement createStatement() throws SQLException {
		this.validateConnectionIsNotClose();
		return this.createStatement(this.getSessionProperties());
	}

	public Statement createStatement(FireboltProperties fireboltProperties) throws SQLException {
		this.validateConnectionIsNotClose();
		FireboltStatement fireboltStatement = FireboltStatement.builder().statementService(fireboltStatementService)
				.sessionProperties(fireboltProperties).connection(this).build();
		this.addStatement(fireboltStatement);
		return fireboltStatement;
	}

	private void addStatement(FireboltStatement statement) throws SQLException {
		synchronized (statements) {
			this.validateConnectionIsNotClose();
			this.statements.add(statement);
		}
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		this.validateConnectionIsNotClose();
		return false;
	}

	@Override
	public boolean isClosed() {
		return this.closed;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		this.validateConnectionIsNotClose();
		return new FireboltDatabaseMetadata(this.httpConnectionUrl, this);
	}

	@Override
	public String getCatalog() throws SQLException {
		this.validateConnectionIsNotClose();
		return sessionProperties.getDatabase();
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		// no-op
	}

	public String getEngine() throws SQLException {
		this.validateConnectionIsNotClose();
		return fireboltEngineService.getEngineNameFromHost(this.getSessionProperties().getHost());
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		this.validateConnectionIsNotClose();
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		this.validateConnectionIsNotClose();
		return createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
	}

	@Override
	public String getSchema() throws SQLException {
		return null;
	}

	@Override
	public void setSchema(String schema) {
		// no-op
	}

	@Override
	public void abort(Executor executor) throws SQLException {
		validateConnectionIsNotClose();
		if (executor == null) {
			throw new FireboltException("Cannot abort: the executor is null");
		}
		if (!this.closed) {
			executor.execute(this::close);
		}
	}

	@Override
	public void close() {
		log.debug("Closing connection");
		synchronized (this) {
			if (this.isClosed()) {
				return;
			} else {
				closed = true;
			}
		}
		synchronized (statements) {
			for (FireboltStatement statement : this.statements) {
				try {
					statement.close(false);
				} catch (Exception e) {
					log.warn("Could not close statement", e);
				}
			}
			statements.clear();
		}
		log.debug("Connection closed");
	}

	private FireboltProperties extractFireboltProperties(String jdbcUri, Properties connectionProperties) {
		Properties propertiesFromUrl = FireboltJdbcUrlUtil.extractProperties(jdbcUri);
		return FireboltProperties.of(propertiesFromUrl, connectionProperties);
	}

	private String getHttpConnectionUrl(FireboltProperties newSessionProperties) {
		String hostAndPort = newSessionProperties.getHost() + ":" + newSessionProperties.getPort();
		return newSessionProperties.isSsl() ? "https://" + hostAndPort : "http://" + hostAndPort;
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		this.validateConnectionIsNotClose();
		if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLFeatureNotSupportedException();
		}
		return createPreparedStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		this.validateConnectionIsNotClose();
		return this.createPreparedStatement(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		this.validateConnectionIsNotClose();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		this.validateConnectionIsNotClose();
		return this.prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		this.validateConnectionIsNotClose();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		this.validateConnectionIsNotClose();
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		this.validateConnectionIsNotClose();
		throw new SQLFeatureNotSupportedException();
	}

	private PreparedStatement createPreparedStatement(String sql) throws SQLException {
		this.validateConnectionIsNotClose();
		FireboltPreparedStatement statement = FireboltPreparedStatement.statementBuilder()
				.statementService(fireboltStatementService).sessionProperties(this.getSessionProperties()).sql(sql)
				.connection(this).build();
		this.addStatement(statement);
		return statement;
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		this.validateConnectionIsNotClose();
		if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY
				|| resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
			throw new SQLFeatureNotSupportedException();
		}
		return createStatement();
	}

	public boolean isValid(int timeout) throws SQLException {
		if (timeout < 0) {
			throw new FireboltException("Timeout value cannot be less than 0");
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

	private void validateConnection(FireboltProperties fireboltProperties) throws SQLException {
		try (Statement s = createStatement(fireboltProperties)) {
			s.execute("SELECT 1");
		} catch (Exception e) {
			log.warn("Connection is not valid", e);
			throw e;
		}
	}

	private void validateConnectionIsNotClose() throws SQLException {
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
			this.sessionProperties.addProperty(property);
		} catch (FireboltException e) {
			throw e;
		} catch (Exception e) {
			throw new FireboltException(
					String.format("Could not set property %s=%s", property.getLeft(), property.getRight()), e);
		}
	}

	@Override
	public void commit() throws SQLException {
		// no-op
	}

	@Override
	public void rollback() throws SQLException {
		// no-op
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) {
		return iface.isAssignableFrom(getClass());
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (iface.isAssignableFrom(getClass())) {
			return iface.cast(this);
		}
		throw new SQLException("Cannot unwrap to " + iface.getName());
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		validateConnectionIsNotClose();
		this.networkTimeout = milliseconds;
	}

	@Override
	public int getNetworkTimeout() {
		return this.networkTimeout;
	}

	public int getConnectionTimeout() {
		return this.connectionTimeout;
	}
}
