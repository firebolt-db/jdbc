package com.firebolt.jdbc.connection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.client.FireboltObjectMapper;
import com.firebolt.jdbc.client.HttpClientConfig;
import com.firebolt.jdbc.client.account.FireboltAccount;
import com.firebolt.jdbc.client.account.FireboltAccountRetriever;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.client.gateway.GatewayUrlResponse;
import com.firebolt.jdbc.client.query.StatementClientImpl;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.exception.FireboltUnsupportedOperationException;
import com.firebolt.jdbc.metadata.FireboltDatabaseMetadata;
import com.firebolt.jdbc.metadata.FireboltSystemEngineDatabaseMetadata;
import com.firebolt.jdbc.service.*;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.statement.preparedstatement.FireboltPreparedStatement;
import com.firebolt.jdbc.util.PropertyUtil;
import lombok.CustomLog;
import lombok.NonNull;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

import static com.firebolt.jdbc.connection.settings.FireboltProperties.SYSTEM_ENGINE_NAME;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

@CustomLog
public class FireboltConnection implements Connection {

	private final FireboltAuthenticationService fireboltAuthenticationService;
	private final FireboltStatementService fireboltStatementService;
	private final FireboltEngineService fireboltEngineService;
	private final FireboltGatewayUrlService fireboltGatewayUrlService;
	private final FireboltAccountIdService fireboltAccountIdService;
	private final String httpConnectionUrl;
	private final List<FireboltStatement> statements;
	private final int connectionTimeout;
	private final boolean systemEngine;
	private boolean closed = true;
	private FireboltProperties sessionProperties;
	private FireboltProperties internalSystemEngineProperties;
	private int networkTimeout;

	//Properties that are used at the beginning of the connection for authentication
	private final FireboltProperties loginProperties;

	public FireboltConnection(@NonNull String url, Properties connectionSettings,
							  FireboltAuthenticationService fireboltAuthenticationService,
							  FireboltGatewayUrlService fireboltGatewayUrlService,
							  FireboltStatementService fireboltStatementService,
							  FireboltEngineService fireboltEngineService,
							  FireboltAccountIdService fireboltAccountIdService) throws FireboltException {
		this.loginProperties = this.extractFireboltProperties(url, connectionSettings);

		this.fireboltAuthenticationService = fireboltAuthenticationService;
		this.fireboltGatewayUrlService = fireboltGatewayUrlService;
		this.httpConnectionUrl = getHttpConnectionUrl(loginProperties);
		this.fireboltStatementService = fireboltStatementService;

		this.statements = new ArrayList<>();
		this.connectionTimeout = loginProperties.getConnectionTimeoutMillis();
		this.networkTimeout = loginProperties.getSocketTimeoutMillis();
		this.systemEngine = loginProperties.isSystemEngine();
		this.fireboltEngineService = fireboltEngineService;
		this.fireboltAccountIdService = fireboltAccountIdService;
		this.connect();
	}

	// This code duplication between constructors is done because of back reference: dependent services require reference to current instance of FireboltConnection that prevents using constructor chaining or factory method.
	@ExcludeFromJacocoGeneratedReport
	public FireboltConnection(@NonNull String url, Properties connectionSettings) throws FireboltException {
		this.loginProperties = extractFireboltProperties(url, connectionSettings);
		OkHttpClient httpClient = getHttpClient(loginProperties);
		ObjectMapper objectMapper = FireboltObjectMapper.getInstance();

		this.fireboltAuthenticationService = new FireboltAuthenticationService(new FireboltAuthenticationClient(httpClient, objectMapper, this, loginProperties.getUserDrivers(), loginProperties.getUserClients()));
		this.fireboltGatewayUrlService = new FireboltGatewayUrlService(createFireboltAccountRetriever(httpClient, objectMapper, "engineUrl", GatewayUrlResponse.class));
		this.httpConnectionUrl = getHttpConnectionUrl(loginProperties);
		this.fireboltStatementService = new FireboltStatementService(new StatementClientImpl(httpClient, objectMapper, this, loginProperties.getUserDrivers(), loginProperties.getUserClients()));

		this.statements = new ArrayList<>();
		this.connectionTimeout = loginProperties.getConnectionTimeoutMillis();
		this.networkTimeout = loginProperties.getSocketTimeoutMillis();
		this.systemEngine = loginProperties.isSystemEngine();
		this.fireboltEngineService = new FireboltEngineService(this);
		this.fireboltAccountIdService = new FireboltAccountIdService(createFireboltAccountRetriever(httpClient, objectMapper, "resolve", FireboltAccount.class));

		this.connect();
	}

	private static OkHttpClient getHttpClient(FireboltProperties fireboltProperties) throws FireboltException {
		try {
			return HttpClientConfig.getInstance() == null ? HttpClientConfig.init(fireboltProperties) : HttpClientConfig.getInstance();
		} catch (GeneralSecurityException | IOException e) {
			throw new FireboltException("Could not instantiate http client", e);
		}
	}

	private <T> FireboltAccountRetriever<T> createFireboltAccountRetriever(OkHttpClient httpClient, ObjectMapper objectMapper, String path, Class<T> type) {
		return new FireboltAccountRetriever<>(httpClient, objectMapper, this, loginProperties.getUserDrivers(), loginProperties.getUserClients(), loginProperties.getHost(), path, type);
	}

	private void connect() throws FireboltException {
		String accessToken = this.getAccessToken(loginProperties).orElse(StringUtils.EMPTY);
		closed = false;
		if (!PropertyUtil.isLocalDb(loginProperties)) {
			internalSystemEngineProperties = createInternalSystemEngineProperties(accessToken, loginProperties.getAccount());
			String accountId = fireboltAccountIdService.getValue(accessToken, loginProperties.getAccount());
			if (!loginProperties.isSystemEngine()) {
				sessionProperties = internalSystemEngineProperties.toBuilder()
						.engine(loginProperties.getEngine())
						.systemEngine(true)
						.accountId(accountId)
						.build();
				sessionProperties = getSessionPropertiesForNonSystemEngine();
			} else {
				//When using system engine, the system engine properties are the same as the session properties
				sessionProperties = internalSystemEngineProperties.toBuilder().accountId(accountId).build();
			}
		} else {
			//When running packdb locally, the login properties are the session properties
			sessionProperties = loginProperties;
		}
		log.debug("Connection opened");
	}

	private FireboltProperties getSessionPropertiesForNonSystemEngine() throws FireboltException {
		Engine engine = fireboltEngineService.getEngine(loginProperties.getEngine(), loginProperties.getDatabase());
		return loginProperties.toBuilder().host(engine.getEndpoint()).engine(engine.getName()).systemEngine(false).build();
	}

	private FireboltProperties createInternalSystemEngineProperties(String accessToken, String account) throws FireboltException {
		String systemEngineEndpoint = fireboltGatewayUrlService.getUrl(accessToken, account);
		return this.loginProperties
				.toBuilder()
				.systemEngine(true)
				.engine(SYSTEM_ENGINE_NAME)
				.compress(false)
				.host(UrlUtil.createUrl(systemEngineEndpoint).getHost()).database(null).build();
	}

	public void removeExpiredTokens() throws FireboltException {
		fireboltAuthenticationService.removeConnectionTokens(httpConnectionUrl, loginProperties);
	}

	public Optional<String> getAccessToken() throws FireboltException {
		return this.getAccessToken(sessionProperties);
	}

	private Optional<String> getAccessToken(FireboltProperties fireboltProperties) throws FireboltException {
		if (!PropertyUtil.isLocalDb(fireboltProperties)) {
			return Optional.of(fireboltAuthenticationService.getConnectionTokens(httpConnectionUrl, fireboltProperties)).map(FireboltConnectionTokens::getAccessToken);
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
		if (!this.systemEngine) {
			return new FireboltDatabaseMetadata(this.httpConnectionUrl, this);
		} else {
			return new FireboltSystemEngineDatabaseMetadata(this.httpConnectionUrl, this);
		}
	}

	@Override
	public String getCatalog() throws SQLException {
		this.validateConnectionIsNotClose();
		return sessionProperties.getDatabase();
	}

	@Override
	@NotImplemented
	public void setCatalog(String catalog) throws SQLException {
		// no-op as catalogs are not supported
	}

	public String getEngine()  {
		return this.getSessionProperties().getEngine();
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		this.validateConnectionIsNotClose();
		return Connection.TRANSACTION_NONE;
	}

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
		Properties propertiesFromUrl = UrlUtil.extractProperties(jdbcUri);
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
			if (!this.systemEngine) {
				validateConnection(this.getSessionProperties(), true);
			}
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

	@Override
	@NotImplemented
	public void commit() throws SQLException {
		// no-op as transactions are not supported
	}

	@Override
	@NotImplemented
	public void rollback() throws SQLException {
		// no-op as transactions are not supported
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

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void setReadOnly(boolean readOnly) throws SQLException {
		// no-op
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public SQLWarning getWarnings() throws SQLException {
		return null;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void clearWarnings() throws SQLException {
		// no-op
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		// Since setTypeMap is currently not supported, an empty map is returned (refer to the doc for more info)
		return new HashMap<>();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@ExcludeFromJacocoGeneratedReport
	public int getHoldability() throws SQLException {
		return 0;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void setHoldability(int holdability) throws SQLException {
		// No support for transaction
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Savepoint setSavepoint() throws SQLException {
		// No support for transaction
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Savepoint setSavepoint(String name) throws SQLException {
		// No support for transaction
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void rollback(Savepoint savepoint) throws SQLException {
		// No support for transaction
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Clob createClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Blob createBlob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public NClob createNClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		// Not supported
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public String getClientInfo(String name) throws SQLException {
		return null;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Properties getClientInfo() throws SQLException {
		return new Properties();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		// Not supported
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}
}
