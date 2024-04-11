package com.firebolt.jdbc.connection;

import com.firebolt.jdbc.JdbcBase;
import com.firebolt.jdbc.annotation.ExcludeFromJacocoGeneratedReport;
import com.firebolt.jdbc.annotation.NotImplemented;
import com.firebolt.jdbc.client.HttpClientConfig;
import com.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import com.firebolt.jdbc.client.query.StatementClientImpl;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.exception.FireboltSQLFeatureNotSupportedException;
import com.firebolt.jdbc.exception.FireboltUnsupportedOperationException;
import com.firebolt.jdbc.metadata.FireboltDatabaseMetadata;
import com.firebolt.jdbc.metadata.FireboltSystemEngineDatabaseMetadata;
import com.firebolt.jdbc.service.FireboltAuthenticationService;
import com.firebolt.jdbc.service.FireboltStatementService;
import com.firebolt.jdbc.statement.FireboltStatement;
import com.firebolt.jdbc.statement.preparedstatement.FireboltPreparedStatement;
import com.firebolt.jdbc.type.FireboltDataType;
import com.firebolt.jdbc.type.array.FireboltArray;
import com.firebolt.jdbc.util.PropertyUtil;
import lombok.CustomLog;
import lombok.NonNull;
import okhttp3.OkHttpClient;

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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

@CustomLog
public abstract class FireboltConnection extends JdbcBase implements Connection {

	private final FireboltAuthenticationService fireboltAuthenticationService;
	private final FireboltStatementService fireboltStatementService;
	protected String httpConnectionUrl;
	private final List<FireboltStatement> statements;
	private final int connectionTimeout;
	private boolean closed = true;
	protected FireboltProperties sessionProperties;
	private int networkTimeout;
	private final String protocolVersion;
	protected int infraVersion = 1;
	private DatabaseMetaData databaseMetaData;

	//Properties that are used at the beginning of the connection for authentication
	protected final FireboltProperties loginProperties;

	protected FireboltConnection(@NonNull String url,
								 Properties connectionSettings,
								 FireboltAuthenticationService fireboltAuthenticationService,
							  	 FireboltStatementService fireboltStatementService,
								 String protocolVersion) {
		this.loginProperties = extractFireboltProperties(url, connectionSettings);

		this.fireboltAuthenticationService = fireboltAuthenticationService;
		this.httpConnectionUrl = loginProperties.getHttpConnectionUrl();
		this.fireboltStatementService = fireboltStatementService;

		this.statements = new ArrayList<>();
		this.connectionTimeout = loginProperties.getConnectionTimeoutMillis();
		this.networkTimeout = loginProperties.getSocketTimeoutMillis();
		this.protocolVersion = protocolVersion;
	}

	// This code duplication between constructors is done because of back reference: dependent services require reference to current instance of FireboltConnection that prevents using constructor chaining or factory method.
	@ExcludeFromJacocoGeneratedReport
	protected FireboltConnection(@NonNull String url, Properties connectionSettings, String protocolVersion) throws SQLException {
		this.loginProperties = extractFireboltProperties(url, connectionSettings);
		OkHttpClient httpClient = getHttpClient(loginProperties);

		this.fireboltAuthenticationService = new FireboltAuthenticationService(createFireboltAuthenticationClient(httpClient));
		this.httpConnectionUrl = loginProperties.getHttpConnectionUrl();
		this.fireboltStatementService = new FireboltStatementService(new StatementClientImpl(httpClient, this, loginProperties.getUserDrivers(), loginProperties.getUserClients()));

		this.statements = new ArrayList<>();
		this.connectionTimeout = loginProperties.getConnectionTimeoutMillis();
		this.networkTimeout = loginProperties.getSocketTimeoutMillis();
		this.protocolVersion = protocolVersion;
	}

	protected abstract FireboltAuthenticationClient createFireboltAuthenticationClient(OkHttpClient httpClient);

	public static FireboltConnection create(@NonNull String url, Properties connectionSettings) throws SQLException {
		return createConnectionInstance(url, connectionSettings);
	}

	private static FireboltConnection createConnectionInstance(@NonNull String url, Properties connectionSettings) throws SQLException {
		switch(getUrlVersion(url, connectionSettings)) {
			case 1: return new FireboltConnectionUserPassword(url, connectionSettings);
			case 2: return new FireboltConnectionServiceSecret(url, connectionSettings);
			default: throw new IllegalArgumentException(format("Cannot distinguish version from url %s", url));
		}
	}

	private static int getUrlVersion(String url, Properties connectionSettings) {
		Pattern urlWithHost = Pattern.compile("jdbc:firebolt://api\\.\\w+\\.firebolt\\.io");
		if (!urlWithHost.matcher(url).find()) {
			return 2; // new URL format
		}
		// old URL format
		Properties propertiesFromUrl = UrlUtil.extractProperties(url);
		Properties allSettings = PropertyUtil.mergeProperties(propertiesFromUrl, connectionSettings);
		if (allSettings.containsKey("client_id") && allSettings.containsKey("client_secret") && !allSettings.containsKey("user") && !allSettings.containsKey("password")) {
			return 2;
		}
		FireboltProperties props = new FireboltProperties(new Properties[] {propertiesFromUrl, connectionSettings});
		String principal = props.getPrincipal();
		if (principal != null && principal.contains("@")) {
			return 1;
		}
		return 2;
	}

	protected OkHttpClient getHttpClient(FireboltProperties fireboltProperties) throws FireboltException {
		try {
			return HttpClientConfig.getInstance() == null ? HttpClientConfig.init(fireboltProperties) : HttpClientConfig.getInstance();
		} catch (GeneralSecurityException | IOException e) {
			throw new FireboltException("Could not instantiate http client", e);
		}
	}

	protected void connect() throws SQLException {
		closed = false;
		if (!PropertyUtil.isLocalDb(loginProperties)) {
			authenticate();
		} else {
			// When running packdb locally, the login properties are the session properties
			sessionProperties = loginProperties;
			// The validation of not local DB is implemented into authenticate() method itself.
			assertDatabaseExisting(loginProperties.getDatabase());
		}
		databaseMetaData = retrieveMetaData();

		log.debug("Connection opened");
	}

	protected abstract void authenticate() throws SQLException;

	protected abstract void assertDatabaseExisting(String database) throws SQLException;

	public void removeExpiredTokens() throws FireboltException {
		fireboltAuthenticationService.removeConnectionTokens(httpConnectionUrl, loginProperties);
	}

	public Optional<String> getAccessToken() throws FireboltException {
		return getAccessToken(sessionProperties);
	}

	protected Optional<String> getAccessToken(FireboltProperties fireboltProperties) throws FireboltException {
		String accessToken = fireboltProperties.getAccessToken();
		if (accessToken != null) {
			if (fireboltProperties.getPrincipal() != null || fireboltProperties.getSecret() != null) {
				throw new FireboltException("Ambiguity: Both access token and client ID/secret are supplied");
			}
			return Optional.of(accessToken);
		}

		if (!PropertyUtil.isLocalDb(fireboltProperties)) {
			return Optional.of(fireboltAuthenticationService.getConnectionTokens(httpConnectionUrl, fireboltProperties)).map(FireboltConnectionTokens::getAccessToken);
		}
		return Optional.empty();
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
	public void setAutoCommit(boolean autoCommit) throws SQLException {
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

	private DatabaseMetaData retrieveMetaData() {
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
	public void setCatalog(String catalog) throws SQLException {
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
		FireboltPreparedStatement statement = new FireboltPreparedStatement(fireboltStatementService, this, sql);
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
			if (!loginProperties.isSystemEngine()) {
				validateConnection(getSessionProperties(), true);
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
			statements.remove(fireboltStatement);
		}
	}

	public void addProperty(@NonNull String key, String value) throws FireboltException {
		changeProperty(p -> p.addProperty(key, value), () -> format("Could not set property %s=%s", key, value));
	}

	public void addProperty(Entry<String, String> property) throws FireboltException {
		changeProperty(p -> p.addProperty(property), () -> format("Could not set property %s=%s", property.getKey(), property.getValue()));
	}

	public void reset() throws FireboltException {
		changeProperty(FireboltProperties::clearAdditionalProperties, () -> "Could not reset connection");
	}

	private synchronized void changeProperty(Consumer<FireboltProperties> propertiesEditor, Supplier<String> errorMessageFactory) throws FireboltException {
		try {
			FireboltProperties tmpProperties = FireboltProperties.copy(sessionProperties);
			propertiesEditor.accept(tmpProperties);
			validateConnection(tmpProperties, false);
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
	public void commit() throws SQLException {
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
	public String nativeSQL(String sql) {
		return sql;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		validateConnectionIsNotClose();
		return false;
	}

	@Override
	@ExcludeFromJacocoGeneratedReport
	@NotImplemented
	public void setReadOnly(boolean readOnly) throws SQLException {
		// no-op
	}

	@Override
	@NotImplemented
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		throw new FireboltSQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public Map<String, Class<?>> getTypeMap() throws SQLException {
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
	public void setHoldability(int holdability) throws SQLException {
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
	@NotImplemented
	public Clob createClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public Blob createBlob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public NClob createNClob() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	@NotImplemented
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		// Not supported
	}

	@Override
	@NotImplemented
	public String getClientInfo(String name) throws SQLException {
		return null;
	}

	@Override
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

	public int getInfraVersion() {
		return infraVersion;
	}
}
