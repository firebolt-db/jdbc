package com.firebolt.jdbc.client.query;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.internal.http2.StreamResetException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.DEFAULT_FORMAT;
import static com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey.OUTPUT_FORMAT;
import static com.firebolt.jdbc.exception.ExceptionType.UNAUTHORIZED;
import static java.lang.String.format;

@CustomLog
public class StatementClientImpl extends FireboltClient implements StatementClient {

	private static final String TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT = "TabSeparatedWithNamesAndTypes";

	private final BiPredicate<Call, String> isCallWithId = (call, id) -> call.request().tag() instanceof String
			&& StringUtils.equals((String) call.request().tag(), id);

	public StatementClientImpl(OkHttpClient httpClient, ObjectMapper objectMapper, FireboltConnection connection, String customDrivers, String customClients) {
		super(httpClient, objectMapper, connection, customDrivers, customClients);
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
		String formattedStatement = formatStatement(statementInfoWrapper);
		Map<String, String> params = getAllParameters(connectionProperties, statementInfoWrapper, systemEngine, queryTimeout);
		try {
			String uri = this.buildQueryUri(connectionProperties, params).toString();
			return executeSqlStatementWithRetryOnUnauthorized(statementInfoWrapper, connectionProperties, formattedStatement, uri);
		} catch (FireboltException e) {
			throw e;
		} catch (StreamResetException e) {
			String errorMessage = format("Error executing statement with id %s: %s", statementInfoWrapper.getId(), formattedStatement);
			throw new FireboltException(errorMessage, e, ExceptionType.CANCELED);
		} catch (Exception e) {
			String errorMessage = format("Error executing statement with id %s: %s", statementInfoWrapper.getId(), formattedStatement);
			throw new FireboltException(errorMessage, e);
		}
	}

	private InputStream executeSqlStatementWithRetryOnUnauthorized(@NonNull StatementInfoWrapper statementInfoWrapper,
																   @NonNull FireboltProperties connectionProperties, String formattedStatement, String uri)
			throws IOException, FireboltException {
		try {
			log.debug("Posting statement with id {} to URI: {}", statementInfoWrapper.getId(), uri);
			return postSqlStatement(statementInfoWrapper, connectionProperties, formattedStatement, uri);
		} catch (FireboltException exception) {
			if (exception.getType() == UNAUTHORIZED) {
				log.debug("Retrying to post statement with id {} following a 401 status code to URI: {}",
						statementInfoWrapper.getId(), uri);
				return postSqlStatement(statementInfoWrapper, connectionProperties, formattedStatement, uri);
			} else {
				throw exception;
			}
		}
	}

	private InputStream postSqlStatement(@NonNull StatementInfoWrapper statementInfoWrapper,
			@NonNull FireboltProperties connectionProperties, String formattedStatement, String uri)
			throws FireboltException, IOException {
		Response response;
		Request post = this.createPostRequest(uri, formattedStatement,
				this.getConnection().getAccessToken().orElse(null), statementInfoWrapper.getId());
		response = this.execute(post, connectionProperties.getHost(), connectionProperties.isCompress());
		InputStream is = Optional.ofNullable(response.body()).map(ResponseBody::byteStream).orElse(null);
		if (is == null) {
			CloseableUtil.close(response);
		}
		return is;
	}

	private String formatStatement(StatementInfoWrapper statementInfoWrapper) {
		Optional<String> cleanSql = Optional.ofNullable(statementInfoWrapper.getInitialStatement())
				.map(RawStatement::getCleanSql);
		if (cleanSql.isPresent() && !StringUtils.endsWith(cleanSql.get(), ";")) {
			return statementInfoWrapper.getSql() + ";";
		} else {
			return statementInfoWrapper.getSql();
		}
	}

	/**
	 * Aborts the statement being sent to the server
	 *
	 * @param id                 id of the statement
	 * @param fireboltProperties the properties
	 */
	public void abortStatement(String id, FireboltProperties fireboltProperties) throws FireboltException {
		try {
			String uri = this.buildCancelUri(fireboltProperties, id).toString();
			Request rq = this.createPostRequest(uri, this.getConnection().getAccessToken().orElse(null), null);
			try (Response response = this.execute(rq, fireboltProperties.getHost())) {
				CloseableUtil.close(response);
			}
		} catch (FireboltException e) {
			if (e.getType() == ExceptionType.INVALID_REQUEST) {
				// 400 on that request indicates that the statement does not exist
				log.warn(e.getMessage());
			} else {
				throw e;
			}
		} catch (Exception e) {
			throw new FireboltException(format("Could not cancel query: %s at %s", id, fireboltProperties.getHost()), e);
		}
	}

	/**
	 * Abort HttpRequest if it is currently being sent
	 *
	 * @param id id of the statement
	 */
	public void abortRunningHttpRequest(@NonNull String id) {
		getQueuedCallWithId(id).ifPresent(Call::cancel);
		getRunningCallWithId(id).ifPresent(Call::cancel);
	}

	private Optional<Call> getQueuedCallWithId(String id) {
		return getSelectedCallWithId(id, Dispatcher::queuedCalls);
	}

	private Optional<Call> getRunningCallWithId(String id) {
		return getSelectedCallWithId(id, Dispatcher::runningCalls);
	}

	private Optional<Call> getSelectedCallWithId(String id, Function<Dispatcher, List<Call>> callsGetter) {
		return callsGetter.apply(getHttpClient().dispatcher()).stream().filter(call -> isCallWithId.test(call, id)).findAny();
	}

	@Override
	public boolean isStatementRunning(String statementId) {
		return getQueuedCallWithId(statementId).isPresent() || getRunningCallWithId(statementId).isPresent();
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
				.ifPresent(format -> params.put(format.getLeft(), format.getRight()));
		if (systemEngine) {
			if (fireboltProperties.getAccountId() != null) {
				params.put(FireboltQueryParameterKey.ACCOUNT_ID.getKey(), fireboltProperties.getAccountId());
			}
		} else {
			params.put(FireboltQueryParameterKey.QUERY_ID.getKey(), statementInfoWrapper.getId());
			params.put(FireboltQueryParameterKey.COMPRESS.getKey(), fireboltProperties.isCompress() ? "1" : "0");
			params.put(FireboltQueryParameterKey.DATABASE.getKey(), fireboltProperties.getDatabase());

			if (queryTimeout > 0) {
				params.put("max_execution_time", String.valueOf(queryTimeout));
			}
		}

		return params;
	}

	private Optional<Pair<String, String>> getResponseFormatParameter(boolean isQuery, boolean isLocalDb) {
		FireboltQueryParameterKey format = isLocalDb ? DEFAULT_FORMAT : OUTPUT_FORMAT;
		return isQuery ? Optional.of(Pair.of(format.getKey(), TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT)) : Optional.empty();
	}

	private Map<String, String> getCancelParameters(String statementId) {
		return Map.of(FireboltQueryParameterKey.QUERY_ID.getKey(), statementId);
	}
}
