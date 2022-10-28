package com.firebolt.jdbc.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.PropertyUtil;
import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.client.FireboltObjectMapper;
import com.firebolt.jdbc.client.HttpClientConfig;
import com.firebolt.jdbc.client.account.FireboltAccountClient;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.client.query.StatementClientImpl;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.exception.FireboltUnsupportedOperationException;
import com.firebolt.jdbc.metadata.FireboltDatabaseMetadata;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltEngineService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.statement.preparedstatement.FireboltPreparedStatement;
import lombok.CustomLog;
import lombok.NonNull;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

@CustomLog
public class FireboltConnection implements Connection {

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

	@ExcludeFromJacocoGeneratedReport
	public FireboltConnection(@NonNull String url, Properties connectionSettings) throws FireboltException {
		ObjectMapper objectMapper = FireboltObjectMapper.getInstance();
		this.loginProperties = this.extractFireboltProperties(url, connectionSettings);
		String driverVersions = loginProperties.getAdditionalProperties().remove("user_drivers");
		String clientVersions = loginProperties.getAdditionalProperties().remove("user_clients");
		this.httpConnectionUrl = getHttpConnectionUrl(loginProperties);
		OkHttpClient httpClient = getHttpClient(loginProperties);
		this.fireboltAuthenticationService = new FireboltAuthenticationService(
				new FireboltAuthenticationClient(httpClient, objectMapper, this, driverVersions, clientVersions, loginProperties.getMaxRetries() ));
		this.fireboltEngineService = new FireboltEngineService(
				new FireboltAccountClient(httpClient, objectMapper, this, driverVersions, clientVersions, loginProperties.getMaxRetries()));
		this.fireboltStatementService = new FireboltStatementService(
				new StatementClientImpl(httpClient, this, objectMapper, driverVersions, clientVersions, loginProperties.getMaxRetries()));
		this.statements = new ArrayList<>();
		this.connectionTimeout = loginProperties.getConnectionTimeoutMillis();
		this.networkTimeout = loginProperties.getSocketTimeoutMillis();
		this.connect();
	}

	private static OkHttpClient getHttpClient(FireboltProperties fireboltProperties)
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

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		// No-op as Firebolt does not support transactions
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

	/** @hidden */
	@Override
	@NotImplemented
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

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void setTransactionIsolation(int level) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		this.validateConnectionIsNotClose();
		if (resultSetType != TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
			throw new FireboltSQLFeatureNotSupportedException();
		}
		return createStatement();
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
		return this.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return this.createPreparedStatement(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		this.validateConnectionIsNotClose();
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		if (resultSetType != TYPE_FORWARD_ONLY) {
			throw new FireboltSQLFeatureNotSupportedException();
		}
		return this.prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		this.validateConnectionIsNotClose();
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		this.validateConnectionIsNotClose();
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		this.validateConnectionIsNotClose();
		throw new FireboltSQLFeatureNotSupportedException();
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
		return this.createStatement(resultSetType, resultSetConcurrency);
	}

	public boolean isValid(int timeout) throws SQLException {
		if (timeout < 0) {
			throw new FireboltException("Timeout value cannot be less than 0");
		}
		if (isClosed()) {
			return false;
		}
		try {
			validateConnection(this.getSessionProperties(), true);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private void validateConnection(FireboltProperties fireboltProperties, boolean ignoreToManyRequestsError)
			throws SQLException {
		try (Statement s = createStatement(fireboltProperties)) {
			s.execute("SELECT 1");
		} catch (Exception e) {
			// A connection is not invalid when too many requests are being sent.
			// This error cannot be ignored when testing the connection to validate a param.
			if (ignoreToManyRequestsError && e instanceof FireboltException
					&& ((FireboltException) e).getType() == ExceptionType.TOO_MANY_REQUESTS) {
				log.warn("Too many requests are being sent to the server", e);
			} else {
				log.warn("Connection is not valid", e);
				throw e;
			}
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
			validateConnection(tmpProperties, false);
			this.sessionProperties.addProperty(property);
		} catch (FireboltException e) {
			throw e;
		} catch (Exception e) {
			throw new FireboltException(
					String.format("Could not set property %s=%s", property.getLeft(), property.getRight()), e);
		}
	}

	/** @hidden */
	@Override
	@NotImplemented
	public void commit() throws SQLException {
		// no-op
	}

	/** @hidden */
	@Override
	@NotImplemented
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

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public String nativeSQL(String sql) throws SQLException {
		throw new FireboltUnsupportedOperationException();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public boolean isReadOnly() throws SQLException {
		return false;
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void setReadOnly(boolean readOnly) throws SQLException {
		// no-op
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void clearWarnings() throws SQLException {
		// no-op
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public int getHoldability() throws SQLException {
		return 0;
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void setHoldability(int holdability) throws SQLException {
		// No support for transaction
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Savepoint setSavepoint() throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Savepoint setSavepoint(String name) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void rollback(Savepoint savepoint) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Clob createClob() throws SQLException {
		return null;
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Blob createBlob() throws SQLException {
		return null;
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public NClob createNClob() throws SQLException {
		return null;
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public SQLXML createSQLXML() throws SQLException {
		return null;
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		// Not supported yet
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public String getClientInfo(String name) throws SQLException {
		return null;
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Properties getClientInfo() throws SQLException {
		return null;
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		// Not supported yet
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	/** @hidden */
	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		return null;
	}
}
