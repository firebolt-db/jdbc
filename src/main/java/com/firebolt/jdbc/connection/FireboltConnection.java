package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.FireboltBackendType;
import com.firebolt.jdbc.JdbcBase;
import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.client.HttpClientConfig;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.client.query.StatementClientImpl;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.connection.settings.FireboltSessionProperty;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.metadata.FireboltDatabaseMetadata;
import com.firebolt.jdbc.metadata.FireboltSystemEngineDatabaseMetadata;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.statement.preparedstatement.FireboltPreparedStatement;
import com.firebolt.jdbc.statement.preparedstatement.FireboltPreparedStatementProvider;
import com.firebolt.jdbc.type.FireboltDataType;
import com.firebolt.jdbc.type.ParserVersion;
import com.firebolt.jdbc.type.array.FireboltArray;
import com.firebolt.jdbc.type.lob.FireboltBlob;
import com.firebolt.jdbc.type.lob.FireboltClob;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.CustomLog;
import lombok.Getter;
import lombok.NonNull;
import okhttp3.OkHttpClient;
import org.apache.commons.lang3.StringUtils;

import static com.firebolt.jdbc.connection.settings.FireboltSessionProperty.getNonDeprecatedProperties;
import static java.lang.String.format;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.util.stream.Collectors.toMap;


@CustomLog
public abstract class FireboltConnection extends JdbcBase implements Connection, CacheListener {

	private static final boolean VALIDATE_CONNECTION = true;
	private static final boolean DO_NOT_VALIDATE_CONNECTION = false;

	private final FireboltAuthenticationService fireboltAuthenticationService;
	private final FireboltStatementService fireboltStatementService;
	protected String httpConnectionUrl;
	private final List<FireboltStatement> statements;
	private final int connectionTimeout;
	private boolean closed = true;
	protected FireboltProperties sessionProperties;
	private int networkTimeout;
	private final String protocolVersion;
	private DatabaseMetaData databaseMetaData;

	//Properties that are used at the beginning of the connection for authentication
	protected final FireboltProperties loginProperties;
	private final Collection<CacheListener> cacheListeners = Collections.newSetFromMap(new IdentityHashMap<>());
	// Parameter parser is determined by the version we're running on
	@Getter
	public final ParserVersion parserVersion;

	protected FireboltConnection(@NonNull String url,
								 Properties connectionSettings,
								 FireboltAuthenticationService fireboltAuthenticationService,
								 FireboltStatementService fireboltStatementService,
			String protocolVersion,
			ParserVersion parserVersion) {
		this.loginProperties = extractFireboltProperties(url, connectionSettings);

		this.fireboltAuthenticationService = fireboltAuthenticationService;
		this.httpConnectionUrl = loginProperties.getHttpConnectionUrl();
		this.fireboltStatementService = fireboltStatementService;

		this.statements = new ArrayList<>();
		this.connectionTimeout = loginProperties.getConnectionTimeoutMillis();
		this.networkTimeout = loginProperties.getSocketTimeoutMillis();
		this.protocolVersion = protocolVersion;
		this.parserVersion = parserVersion;
	}

	// This code duplication between constructors is done because of back reference: dependent services require reference to current instance of FireboltConnection that prevents using constructor chaining or factory method.
	@ExcludeFromJacocoGeneratedReport
	protected FireboltConnection(@NonNull String url, Properties connectionSettings, String protocolVersion,
			ParserVersion parserVersion) throws SQLException {
		this.loginProperties = extractFireboltProperties(url, connectionSettings);
		OkHttpClient httpClient = getHttpClient(loginProperties);

		this.fireboltAuthenticationService = new FireboltAuthenticationService(createFireboltAuthenticationClient(httpClient));
		this.httpConnectionUrl = loginProperties.getHttpConnectionUrl();
		this.fireboltStatementService = new FireboltStatementService(new StatementClientImpl(httpClient, this, loginProperties.getUserDrivers(), loginProperties.getUserClients()));

		this.statements = new ArrayList<>();
		this.connectionTimeout = loginProperties.getConnectionTimeoutMillis();
		this.networkTimeout = loginProperties.getSocketTimeoutMillis();
		this.protocolVersion = protocolVersion;
		this.parserVersion = parserVersion;
	}

	protected abstract FireboltAuthenticationClient createFireboltAuthenticationClient(OkHttpClient httpClient);

	protected OkHttpClient getHttpClient(FireboltProperties fireboltProperties) throws SQLException {
		try {
			return HttpClientConfig.getInstance() == null ? HttpClientConfig.init(fireboltProperties) : HttpClientConfig.getInstance();
		} catch (GeneralSecurityException | IOException e) {
			throw new FireboltException("Could not instantiate http client", e);
		}
	}

	protected void connect() throws SQLException {
		closed = false;

		validateConnectionParameters();

		// try to authenticate
		authenticate();

		databaseMetaData = retrieveMetaData();

		log.debug("Connection opened");
	}

	/**
	 * Returns the version of the firebolt backend the connection is established to
	 */
	public abstract int getInfraVersion();

	protected abstract void authenticate() throws SQLException;

	/**
	 * Validates that the required parameters are present for the connection
	 * @throws SQLException
	 */
	protected abstract void validateConnectionParameters() throws SQLException;

	/**
	 * If the connection information can be cached for subsequent reuse, then the specific connection should provide implementation
	 * @return - true if the connection supports caching. False otherwise
	 */
	protected abstract boolean isConnectionCachingEnabled();

	/**
	 * A connection should implement this method if it needs to set additional details on the user agent header for the calls sent to Firebolt backend
	 * @return
	 */
	public abstract Optional<String> getConnectionUserAgentHeader();

	/**
	 * Returns the backend type that the connection is established to
	 * @return
	 */
	public abstract FireboltBackendType getBackendType();

	public void removeExpiredTokens() throws SQLException {
		fireboltAuthenticationService.removeConnectionTokens(httpConnectionUrl, loginProperties);
	}

	public Optional<String> getAccessToken() throws SQLException {
		return getAccessToken(sessionProperties);
	}

	protected Optional<String> getAccessToken(FireboltProperties fireboltProperties) throws SQLException {
		return Optional.of(fireboltAuthenticationService.getConnectionTokens(httpConnectionUrl, fireboltProperties)).map(FireboltConnectionTokens::getAccessToken);
	}

	public FireboltProperties getSessionProperties() {
		return sessionProperties;
	}

	@Override
	public Statement createStatement() throws SQLException {
		validateConnectionIsNotClose();
		return createStatement(getSessionProperties());
	}

	private Statement createStatement(FireboltProperties fireboltProperties) throws SQLException {
		validateConnectionIsNotClose();
		FireboltStatement fireboltStatement = new FireboltStatement(fireboltStatementService, fireboltProperties, this);
		addStatement(fireboltStatement);
		return fireboltStatement;
	}

	private void addStatement(FireboltStatement statement) throws SQLException {
		synchronized (statements) {
			validateConnectionIsNotClose();
			statements.add(statement);
		}
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		validateConnectionIsNotClose();
		return true;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void setAutoCommit(boolean autoCommit) {
		// No-op as Firebolt does not support transactions
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		validateConnectionIsNotClose();
		return databaseMetaData;
	}

	protected DatabaseMetaData retrieveMetaData() {
		if (!loginProperties.isSystemEngine()) {
			return new FireboltDatabaseMetadata(httpConnectionUrl, this);
		} else {
			return new FireboltSystemEngineDatabaseMetadata(httpConnectionUrl, this);
		}
	}

	@Override
	public String getCatalog() throws SQLException {
		validateConnectionIsNotClose();
		return sessionProperties.getDatabase();
	}

	@Override
	@NotImplemented
	public void setCatalog(String catalog) {
		// no-op as catalogs are not supported
	}

	public String getEngine()  {
		return getSessionProperties().getEngine();
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		validateConnectionIsNotClose();
		return Connection.TRANSACTION_NONE;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		if (level != Connection.TRANSACTION_NONE) {
			throw new FireboltSQLFeatureNotSupportedException();
		}
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		validateConnectionIsNotClose();
		if (resultSetType != TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
			throw new FireboltSQLFeatureNotSupportedException();
		}
		return createStatement();
	}

	@Override
	public String getSchema() throws SQLException {
		validateConnectionIsNotClose();
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
		if (!closed) {
			executor.execute(this::close);
		}
	}

	@Override
	public void close() {
		log.debug("Closing connection");
		synchronized (this) {
			if (isClosed()) {
				return;
			} else {
				closed = true;
			}
		}
		synchronized (statements) {
			for (FireboltStatement statement : statements) {
				try {
					statement.close(false);
				} catch (Exception e) {
					log.warn("Could not close statement", e);
				}
			}
			statements.clear();
		}
		databaseMetaData = null;
		log.debug("Connection closed");
	}

	protected FireboltProperties extractFireboltProperties(String jdbcUri, Properties connectionProperties) {
		return createFireboltProperties(jdbcUri, connectionProperties);
	}

	private static FireboltProperties createFireboltProperties(String jdbcUri, Properties connectionProperties) {
		return new FireboltProperties(new Properties[] {UrlUtil.extractProperties(jdbcUri), connectionProperties});
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
											  int resultSetHoldability) throws SQLException {
		return prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return createPreparedStatement(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		validateConnectionIsNotClose();
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
			throws SQLException {
		if (resultSetType != TYPE_FORWARD_ONLY) {
			throw new FireboltSQLFeatureNotSupportedException();
		}
		return prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
		validateConnectionIsNotClose();
		if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
			throw new FireboltSQLFeatureNotSupportedException();
		}
		return prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		validateConnectionIsNotClose();
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		validateConnectionIsNotClose();
		throw new FireboltSQLFeatureNotSupportedException();
	}

	private PreparedStatement createPreparedStatement(String sql) throws SQLException {
		validateConnectionIsNotClose();
		FireboltPreparedStatementProvider preparedStatementProvider = FireboltPreparedStatementProvider.getInstance();
		FireboltPreparedStatement statement = preparedStatementProvider.getPreparedStatement(sessionProperties,
				this,
				fireboltStatementService,
				sql);
		addStatement(statement);
		return statement;
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return createStatement(resultSetType, resultSetConcurrency);
	}

	public boolean isValid(int timeout) throws SQLException {
		if (timeout < 0) {
			throw new FireboltException("Timeout value cannot be less than 0");
		}
		if (isClosed()) {
			return false;
		}
		try {
			validateConnection(getSessionProperties(), true, true);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private void validateConnection(FireboltProperties fireboltProperties, boolean ignoreToManyRequestsError, boolean isInternalRequest)
			throws SQLException {
		HashMap<String, String> runtimeProperties = new HashMap<>(fireboltProperties.getRuntimeAdditionalProperties());
		if (isInternalRequest) {
			prepareInternalRequestValidationConnection(runtimeProperties);
		}

		var propertiesBuilder = fireboltProperties.toBuilder().runtimeAdditionalProperties(runtimeProperties);
		if (getSessionProperties().isValidateOnSystemEngine()) {
			propertiesBuilder.compress(false).engine(null).systemEngine(true);
		}
		try (Statement s = createStatement(propertiesBuilder.build())) {
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

	protected void prepareInternalRequestValidationConnection(Map<String, String> runtimeProperties) {
		runtimeProperties.put("auto_start_stop_control", "ignore");
	}

	private void validateConnectionIsNotClose() throws SQLException {
		if (isClosed()) {
			throw new FireboltException("Cannot proceed: connection closed");
		}
	}

	public void removeClosedStatement(FireboltStatement fireboltStatement) {
		synchronized (statements) {
			statements.remove(fireboltStatement);
		}
	}

	/**
	 * By default, when adding a property it is validating the connection
	 */
	public void addProperty(@NonNull String key, String value) throws SQLException {
		addProperty(key, value, VALIDATE_CONNECTION);
	}

	public void addProperty(@NonNull String key, String value, boolean validateConnection) throws SQLException {
		changeProperty(p -> p.addProperty(key, value), () -> format("Could not set property %s=%s", key, value), validateConnection);
	}

	public void addProperty(Entry<String, String> property) throws SQLException {
		addProperty(property, VALIDATE_CONNECTION);
	}

	public void addProperty(Entry<String, String> property, boolean validateConnection) throws SQLException {
		changeProperty(p -> p.addProperty(property), () -> format("Could not set property %s=%s", property.getKey(), property.getValue()), validateConnection);
	}

	/**
	 * Server side lets us know when the connection needs to be reset. so no need to validate the connection.
	 */
	public void reset() throws SQLException {
		changeProperty(FireboltProperties::clearAdditionalProperties, () -> "Could not reset connection", DO_NOT_VALIDATE_CONNECTION);
	}

	/**
	 * Certain values we set on the session properties that come from the server (in the header responses). In these cases we don't need to validate the connection
	 * @param propertiesEditor
	 * @param errorMessageFactory
	 * @param validateConnection
	 * @throws SQLException
	 */
	private synchronized void changeProperty(Consumer<FireboltProperties> propertiesEditor, Supplier<String> errorMessageFactory, boolean validateConnection) throws SQLException {
		try {
			FireboltProperties tmpProperties = FireboltProperties.copy(sessionProperties);
			propertiesEditor.accept(tmpProperties);
			if (validateConnection) {
				validateConnection(tmpProperties, false, false);
			}
			propertiesEditor.accept(sessionProperties);
		} catch (FireboltException e) {
			throw e;
		} catch (Exception e) {
			throw new FireboltException(errorMessageFactory.get(), e);
		}
	}

	public void setEndpoint(String endpoint) {
		this.httpConnectionUrl = endpoint;
	}

	public String getEndpoint() {
		return httpConnectionUrl;
	}

	@Override
	@NotImplemented
	public void commit() {
		// no-op as transactions are not supported
	}

	@Override
	@NotImplemented
	public void rollback() throws SQLException {
		// no-op as transactions are not supported
	}

	@Override
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		validateConnectionIsNotClose();
		networkTimeout = milliseconds;
	}

	@Override
	public int getNetworkTimeout() {
		return networkTimeout;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		return translateSQL(sql, true);
	}

	private String translateSQL(String sql, boolean escapeProcessing) throws SQLException {
		if (sql == null) {
			throw new IllegalArgumentException("SQL is null");
		}
		if (!escapeProcessing || sql.indexOf('{') < 0) {
			return sql;
		}
		throw new SQLWarning("Escape processing is not supported right now", "0A000");
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		validateConnectionIsNotClose();
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void setReadOnly(boolean readOnly) {
		// no-op
	}

	@Override
	@NotImplemented
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public Map<String, Class<?>> getTypeMap() {
		// Since setTypeMap is currently not supported, an empty map is returned (refer to the doc for more info)
		return Map.of();
	}

	@Override
	@NotImplemented
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	public int getHoldability() throws SQLException {
		validateConnectionIsNotClose();
		return CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void setHoldability(int holdability) {
		// No support for transaction
	}

	@Override
	@NotImplemented
	public Savepoint setSavepoint() throws SQLException {
		// No support for transaction
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public Savepoint setSavepoint(String name) throws SQLException {
		// No support for transaction
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void rollback(Savepoint savepoint) throws SQLException {
		// No support for transaction
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	public Clob createClob() {
		return new FireboltClob();
	}

	@Override
	public Blob createBlob() {
		return new FireboltBlob();
	}

	@Override
	public NClob createNClob() {
		return new FireboltClob();
	}

	@Override
	@NotImplemented
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		// Not supported
	}

	@Override
	public String getClientInfo(String name) {
		return Optional.ofNullable(FireboltSessionProperty.byAlias(name.toUpperCase()).getValue(sessionProperties)).map(Object::toString).orElse(null);
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		return getNonDeprecatedProperties().stream()
				.filter(key -> key.getValue(sessionProperties) != null)
				.collect(toMap(FireboltSessionProperty::getKey, key -> key.getValue(sessionProperties).toString(), (o, t) -> t, Properties::new));
	}

	@Override
	@NotImplemented
	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		// Not supported
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		validateConnectionIsNotClose();
		return new FireboltArray(FireboltDataType.ofType(typeName), elements);
	}

	@Override
	@NotImplemented
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	public String getProtocolVersion() {
		return protocolVersion;
	}

	public void register(CacheListener listener) {
		cacheListeners.add(listener);
	}

	@Override
	public void cleanup() {
		cacheListeners.forEach(CacheListener::cleanup);
	}

	public boolean isAsyncQueryRunning(String asyncQueryToken) throws SQLException {
		return getAsyncQueryStatus(asyncQueryToken).equals("RUNNING");
	}

	public boolean isAsyncQuerySuccessful(String asyncQueryToken) throws SQLException {
		return getAsyncQueryStatus(asyncQueryToken).equals("ENDED_SUCCESSFULLY");
	}

	public boolean cancelAsyncQuery(String asyncQueryToken) throws SQLException {
		if (StringUtils.isBlank(asyncQueryToken)) {
			throw new FireboltException("Async query token cannot be null or empty");
		}
		String asyncQueryId;
		try (PreparedStatement statement = createPreparedStatement("CALL fb_GetAsyncStatus(?)")) {
			statement.setString(1, asyncQueryToken);
			try (ResultSet rs = statement.executeQuery()) {
				if (!rs.next()) {
					throw new FireboltException("Could not get query_id for the async query with token: " + asyncQueryToken);
				}
				asyncQueryId = rs.getString("query_id");
			}
		}
		if (StringUtils.isNotBlank(asyncQueryId)) {
			try (PreparedStatement statement = createPreparedStatement("CANCEL QUERY WHERE query_id = ?")) {
				statement.setString(1, asyncQueryId);
				statement.executeUpdate();
			}
		} else {
			throw new FireboltException("Could not cancel the async query: query_id is null or empty");
		}
		return true;
	}

	private String getAsyncQueryStatus(String asyncQueryToken) throws SQLException {
		if (StringUtils.isBlank(asyncQueryToken)) {
			throw new FireboltException("Async query token cannot be null or empty");
		}
		try (PreparedStatement statement = prepareStatement("CALL fb_GetAsyncStatus(?)")) {
			statement.setString(1, asyncQueryToken);
			try (ResultSet rs = statement.executeQuery()) {
				if (!rs.next()) {
					throw new FireboltException("Could not get status for the async query with token: " + asyncQueryToken);
				}
				return rs.getString("status");
			}
		} catch (SQLException ex) {
			throw new FireboltException("Could not check the status of the async query", ex);
		}
	}
}
