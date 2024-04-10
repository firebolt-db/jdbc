package com.firebolt.jdbc.client.query;

import com.firebolt.jdbc.client.FireboltClient;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementType;
import com.firebolt.jdbc.statement.rawstatement.RawStatement;
import com.firebolt.jdbc.util.CloseableUtil;
import com.firebolt.jdbc.util.PropertyUtil;
import lombok.CustomLog;
import lombok.NonNull;
import okhttp3.Call;
import okhttp3.Dispatcher;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.internal.http2.StreamResetException;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.regex.Pattern;

import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.DEFAULT_FORMAT;
import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.OUTPUT_FORMAT;
import static com.firebolt.jdbc.exception.ExceptionType.INVALID_REQUEST;
import static com.firebolt.jdbc.exception.ExceptionType.UNAUTHORIZED;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.util.Optional.ofNullable;

@CustomLog
public class StatementClientImpl extends FireboltClient implements StatementClient {

	private static final String TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT = "TabSeparatedWithNamesAndTypes";
	private static final Map<Pattern, String> missConfigurationErrorMessages = Map.of(
			Pattern.compile("HTTP status code: 401"), "Please associate user with your service account.",
			Pattern.compile("Engine .+? does not exist or not authorized"), "Please grant at least one role to user associated your service account."
	);

	private final BiPredicate<Call, String> isCallWithLabel = (call, label) -> call.request().tag() instanceof String && Objects.equals(call.request().tag(), label);
	// visible for testing
	static final String HEADER_UPDATE_PARAMETER = "Firebolt-Update-Parameters";
	static final String HEADER_UPDATE_ENDPOINT = "Firebolt-Update-Endpoint";
	static final String HEADER_RESET_SESSION = "Firebolt-Reset-Session";

	private enum QueryIdFetcher {
		/**
		 * Attach label to statement using trailing comment. This is a hack because label cannot be normally attached to
		 * statement in old version
		 */
		COMMENT {
			@Override
			String formatStatement(StatementInfoWrapper statementInfoWrapper) {
				return QUERY_LABEL.formatStatement(statementInfoWrapper) + "--label:" + statementInfoWrapper.getLabel();
			}

			@Override
			String queryIdFetcher() {
				return "select query_id from information_schema.query_history where status = 'STARTED_EXECUTION' and query_text like ?";
			}

			@Override
			String queryIdLabel(String label) {
				return "%label:" + label;
			}
		},
		/**
		 * Attach label to query using special request parameters {@code query_label}
		 */
		QUERY_LABEL {
			@Override
			String formatStatement(StatementInfoWrapper statementInfoWrapper) {
				return ofNullable(statementInfoWrapper.getInitialStatement()).map(RawStatement::getCleanSql)
						.filter(cleanSql -> !cleanSql.endsWith(";"))
						.map(cleanSql -> statementInfoWrapper.getSql() + ";")
						.orElse(statementInfoWrapper.getSql());
			}

			@Override
			String queryIdFetcher() {
				return "select query_id from information_schema.engine_query_history where status = 'STARTED_EXECUTION' and query_label = ?";
			}

			@Override
			String queryIdLabel(String label) {
				return label;
			}
		},
		;

		abstract String formatStatement(StatementInfoWrapper statementInfoWrapper);
		abstract String queryIdFetcher();
		abstract String queryIdLabel(String label);

		static QueryIdFetcher getQueryFetcher(int infraVersion) {
			return infraVersion < 2 ? COMMENT : QUERY_LABEL;
		}

	}

	public StatementClientImpl(OkHttpClient httpClient, FireboltConnection connection, String customDrivers, String customClients) {
		super(httpClient, connection, customDrivers, customClients);
	}

	/**
	 * Sends SQL statement to Firebolt Retries to send the statement if the first
	 * execution is unauthorized
	 *
	 * @param statementInfoWrapper the statement wrapper
	 * @param connectionProperties the connection properties
	 * @param systemEngine         indicates if system engine is used
	 * @param queryTimeout         query timeout
	 * @param standardSql          indicates if standard sql should be used
	 * @return the server response
	 */
	@Override
	public InputStream executeSqlStatement(@NonNull StatementInfoWrapper statementInfoWrapper,
										   @NonNull FireboltProperties connectionProperties, boolean systemEngine, int queryTimeout,
										   boolean standardSql) throws FireboltException {
		QueryIdFetcher.getQueryFetcher(connection.getInfraVersion()).formatStatement(statementInfoWrapper);
		String formattedStatement = QueryIdFetcher.getQueryFetcher(connection.getInfraVersion()).formatStatement(statementInfoWrapper);
		Map<String, String> params = getAllParameters(connectionProperties, statementInfoWrapper, systemEngine, queryTimeout);
		String label = statementInfoWrapper.getLabel();
		String errorMessage = format("Error executing statement with label %s: %s", label, formattedStatement);
		try {
			String uri = buildQueryUri(connectionProperties, params).toString();
			return executeSqlStatementWithRetryOnUnauthorized(label, connectionProperties, formattedStatement, uri);
		} catch (FireboltException e) {
			throw e;
		} catch (StreamResetException e) {
			throw new FireboltException(errorMessage, e, ExceptionType.CANCELED);
		} catch (Exception e) {
			throw new FireboltException(errorMessage, e);
		}
	}

	private InputStream executeSqlStatementWithRetryOnUnauthorized(String label, @NonNull FireboltProperties connectionProperties, String formattedStatement, String uri)
			throws IOException, FireboltException {
		try {
			log.debug("Posting statement with label {} to URI: {}", label, uri);
			return postSqlStatement(connectionProperties, formattedStatement, uri, label);
		} catch (FireboltException exception) {
			if (exception.getType() == UNAUTHORIZED) {
				log.debug("Retrying to post statement with label {} following a 401 status code to URI: {}",label, uri);
				return postSqlStatement(connectionProperties, formattedStatement, uri, label);
			} else {
				throw exception;
			}
		}
	}

	private InputStream postSqlStatement(@NonNull FireboltProperties connectionProperties, String formattedStatement, String uri, String label)
			throws FireboltException, IOException {
		Request post = createPostRequest(uri, label, formattedStatement, getConnection().getAccessToken().orElse(null));
		Response response = execute(post, connectionProperties.getHost(), connectionProperties.isCompress());
		InputStream is = ofNullable(response.body()).map(ResponseBody::byteStream).orElse(null);
		if (is == null) {
			CloseableUtil.close(response);
		}
		return is;
	}

	public void abortStatement(@NonNull String statementLabel, @NonNull FireboltProperties properties) throws FireboltException {
		boolean aborted = abortRunningHttpRequest(statementLabel);
		if (properties.isSystemEngine()) {
			throw new FireboltException("Cannot cancel a statement using a system engine", INVALID_REQUEST);
		} else {
			abortRunningDbStatement(statementLabel, properties, aborted ? 10_000 : 1);
		}
	}

	/**
	 * Aborts the statement being sent to the server
	 *
	 * @param label				 label of the statement
	 * @param fireboltProperties the properties
	 */
	private void abortRunningDbStatement(String label, FireboltProperties fireboltProperties, int getIdTimeout) throws FireboltException {
		try {
			String id;
			int attempt = 0;
			int getIdAttempts = 10;
			int getIdDelay = Math.max(getIdTimeout / getIdAttempts, 1);
			// Statement ID is retrieved from query_history table. Records are written to this table asynchronously.
			// So, if cancel() is called immediately after executing the statement sometimes the record in query_history
			// can be unavailable. To retrieve it we perform several attempts.
			for (id = getStatementId(label); attempt < getIdAttempts; id = getStatementId(label), attempt++) {
				if (id != null) {
					break;
				}
				delay(getIdDelay);
			}
			if (id == null) {
				throw new FireboltException("Cannot retrieve id for statement with label " + label);
			}
			String uri = buildCancelUri(fireboltProperties, id).toString();
			Request rq = createPostRequest(uri, null, (RequestBody)null, getConnection().getAccessToken().orElse(null));
			try (Response response = execute(rq, fireboltProperties.getHost())) {
				CloseableUtil.close(response);
			}
		} catch (FireboltException e) {
			if (e.getType() == ExceptionType.INVALID_REQUEST || e.getType() == ExceptionType.RESOURCE_NOT_FOUND) {
				// 400 on that request indicates that the statement does not exist
				// 404 - the same when working against "real" v2 engine
				log.warn(e.getMessage());
			} else {
				throw e;
			}
		} catch (Exception e) {
			throw new FireboltException(format("Could not cancel query: %s at %s", label, fireboltProperties.getHost()), e);
		}
	}

	@SuppressWarnings("java:S2142") // "InterruptedException" and "ThreadDeath" should not be ignored
	private void delay(int delay) {
		try {
			Thread.sleep(delay);
		} catch (InterruptedException e) {
			// ignore interrupted exception
		}
	}

	/**
	 * Abort HttpRequest if it is currently being sent
	 *
	 * @param label id of the statement
	 */
	private boolean abortRunningHttpRequest(@NonNull String label) {
		boolean quedAborted = abortCall(getQueuedCallWithLabel(label));
		boolean runningAborted = abortCall(getRunningCallWithLabel(label));
		return quedAborted || runningAborted;
	}

	private boolean abortCall(Optional<Call> call) {
		return call.map(c -> {
			c.cancel();
			return true;
		}).orElse(false);
	}

	private Optional<Call> getQueuedCallWithLabel(String label) {
		return getSelectedCallWithLabel(label, Dispatcher::queuedCalls);
	}

	private Optional<Call> getRunningCallWithLabel(String id) {
		return getSelectedCallWithLabel(id, Dispatcher::runningCalls);
	}

	private Optional<Call> getSelectedCallWithLabel(String label, Function<Dispatcher, List<Call>> callsGetter) {
		return callsGetter.apply(getHttpClient().dispatcher()).stream().filter(call -> isCallWithLabel.test(call, label)).findAny();
	}

	@Override
	public boolean isStatementRunning(String statementId) {
		return getQueuedCallWithLabel(statementId).isPresent() || getRunningCallWithLabel(statementId).isPresent();
	}

	private URI buildQueryUri(FireboltProperties fireboltProperties, Map<String, String> parameters) {
		return buildURI(fireboltProperties, parameters, Collections.emptyList());
	}

	private URI buildCancelUri(FireboltProperties fireboltProperties, String id) {
		Map<String, String> params = getCancelParameters(id);
		return buildURI(fireboltProperties, params, Collections.singletonList("cancel"));
	}

	private URI buildURI(FireboltProperties fireboltProperties, Map<String, String> parameters,
			List<String> pathSegments) {
		HttpUrl.Builder httpUrlBuilder = new HttpUrl.Builder()
				.scheme(fireboltProperties.isSsl() ? "https" : "http")
				.host(fireboltProperties.getHost())
				.port(fireboltProperties.getPort());
		parameters.forEach(httpUrlBuilder::addQueryParameter);

		pathSegments.forEach(httpUrlBuilder::addPathSegment);
		return httpUrlBuilder.build().uri();

	}

	private Map<String, String> getAllParameters(FireboltProperties fireboltProperties,
			StatementInfoWrapper statementInfoWrapper, boolean systemEngine, int queryTimeout) {
		boolean isLocalDb = PropertyUtil.isLocalDb(fireboltProperties);

		Map<String, String> params = new HashMap<>(fireboltProperties.getAdditionalProperties());

		getResponseFormatParameter(statementInfoWrapper.getType() == StatementType.QUERY, isLocalDb)
				.ifPresent(format -> params.put(format.getKey(), format.getValue()));

		String accountId = fireboltProperties.getAccountId();
		if (systemEngine) {
			if (accountId != null && connection.getInfraVersion() < 2) {
				// if infra version >= 2 we should add account_id only if it was supplied by system URL returned from server.
				// In this case it will be in additionalProperties anyway.
				params.put(FireboltQueryParameterKey.ACCOUNT_ID.getKey(), accountId);
			}
		} else {
			if (connection.getInfraVersion() >= 2) {
				if (accountId != null) {
					params.put(FireboltQueryParameterKey.ACCOUNT_ID.getKey(), accountId);
					params.put(FireboltQueryParameterKey.ENGINE.getKey(), fireboltProperties.getEngine());
				}
				params.put(FireboltQueryParameterKey.QUERY_LABEL.getKey(), statementInfoWrapper.getLabel()); //QUERY_LABEL
			}
			params.put(FireboltQueryParameterKey.COMPRESS.getKey(), fireboltProperties.isCompress() ? "1" : "0");

			if (queryTimeout > 0) {
				params.put("max_execution_time", String.valueOf(queryTimeout));
			}
		}
		params.put(FireboltQueryParameterKey.DATABASE.getKey(), fireboltProperties.getDatabase());

		return params;
	}

	private Optional<Entry<String, String>> getResponseFormatParameter(boolean isQuery, boolean isLocalDb) {
		FireboltQueryParameterKey format = isLocalDb ? DEFAULT_FORMAT : OUTPUT_FORMAT;
		return isQuery ? Optional.of(Map.entry(format.getKey(), TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT)) : Optional.empty();
	}

	private Map<String, String> getCancelParameters(String statementId) {
		return Map.of(FireboltQueryParameterKey.QUERY_ID.getKey(), statementId);
	}

	@Override
	protected void validateResponse(String host, Response response, Boolean isCompress) throws FireboltException {
		super.validateResponse(host, response, isCompress);
		FireboltConnection connection = getConnection();
		if (isCallSuccessful(response.code())) {
			if (response.header(HEADER_RESET_SESSION) != null) {
				connection.reset();
			}
			String endpoint = response.header(HEADER_UPDATE_ENDPOINT);
			if (endpoint != null) {
				connection.setEndpoint(connection.getSessionProperties().processEngineUrl(endpoint));
			}
			for (String header : response.headers(HEADER_UPDATE_PARAMETER)) {
				String[] keyValue = header.split("=");
				connection.addProperty(keyValue[0].trim(), keyValue[1].trim());
			}
		}
	}

	@Override
	protected void validateResponse(String host, int statusCode, String errorMessageFromServer) throws FireboltException {
		if (statusCode == HTTP_INTERNAL_ERROR) {
			FireboltException ex = missConfigurationErrorMessages.entrySet().stream()
					.filter(msg -> msg.getKey().matcher(errorMessageFromServer).find()).findFirst()
					.map(msg -> new FireboltException(format("Could not query Firebolt at %s. %s", host, msg.getValue()), HTTP_UNAUTHORIZED, errorMessageFromServer))
					.orElse(null);
			if (ex != null) {
				throw ex;
			}
		}
	}

	private String getStatementId(String label) throws SQLException {
		QueryIdFetcher queryIdFetcher = QueryIdFetcher.getQueryFetcher(connection.getInfraVersion());
		try (PreparedStatement ps = connection.prepareStatement(queryIdFetcher.queryIdFetcher())) {
			ps.setString(1, queryIdFetcher.queryIdLabel(label));
			try (ResultSet rs = ps.executeQuery()) {
				return rs.next() ? rs.getString(1) : null;
			}
		}
	}
}
