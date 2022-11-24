package com.firebolt.jdbc.client.query;

import java.io.InputStream;
import java.net.URI;
import java.util.*;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.CloseableUtil;
import com.firebolt.jdbc.PropertyUtil;
import com.firebolt.jdbc.client.FireboltClient;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.FireboltConnectionTokens;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementType;
import com.firebolt.jdbc.statement.rawstatement.RawStatement;
import com.google.common.collect.ImmutableMap;

import lombok.CustomLog;
import lombok.NonNull;
import okhttp3.*;
import okhttp3.internal.http2.StreamResetException;

@CustomLog
public class StatementClientImpl extends FireboltClient implements StatementClient {

	private static final String TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT = "TabSeparatedWithNamesAndTypes";

	private final BiPredicate<Call, String> isCallWithId = (call, id) -> call.request().tag() instanceof String
			&& StringUtils.equals((String) call.request().tag(), id);

	public StatementClientImpl(OkHttpClient httpClient, FireboltConnection connection, ObjectMapper objectMapper,
			String customDrivers, String customClients) {
		super(httpClient, connection, customDrivers, customClients, objectMapper);
	}

	/**
	 * Sends SQL statement to Firebolt
	 *
	 * @param statementInfoWrapper the statement wrapper
	 * @param connectionProperties the connection properties
	 * @param systemEngine         indicates if system engine is used
	 * @param queryTimeout         query timeout
	 * @param maxRows              max rows
	 * @param standardSql          indicates if standard sql should be used
	 * @return the server response
	 */
	@Override
	public InputStream postSqlStatement(@NonNull StatementInfoWrapper statementInfoWrapper,
			@NonNull FireboltProperties connectionProperties, boolean systemEngine, int queryTimeout, int maxRows,
			boolean standardSql) throws FireboltException {
		String formattedStatement = formatStatement(statementInfoWrapper);
		Map<String, String> params = getAllParameters(connectionProperties, statementInfoWrapper, systemEngine,
				queryTimeout, maxRows, standardSql);

		try {
			String uri = this.buildQueryUri(connectionProperties, params).toString();
			Request post = this.createPostRequest(uri, formattedStatement, this.getConnection().getConnectionTokens()
					.map(FireboltConnectionTokens::getAccessToken).orElse(null), statementInfoWrapper.getId());
			log.debug("Posting statement with id {} to URI: {}", statementInfoWrapper.getId(), uri);
			Response response = this.execute(post, connectionProperties.getHost(), connectionProperties.isCompress());

			return response.body() != null ? response.body().byteStream() : null;
		} catch (FireboltException e) {
			throw e;
		} catch (Exception e) {
			String errorMessage = String.format("Error executing statement with id %s: %s",
					statementInfoWrapper.getId(), formattedStatement);
			if (e instanceof StreamResetException) {
				throw new FireboltException(errorMessage, e, ExceptionType.CANCELED);
			}
			throw new FireboltException(errorMessage, e);
		}

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
			Request rq = this.createPostRequest(uri, this.getConnection().getConnectionTokens()
					.map(FireboltConnectionTokens::getAccessToken).orElse(null), null);
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
			throw new FireboltException(
					String.format("Could not cancel query: %s at %s", id, fireboltProperties.getHost()), e);
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

	Optional<Call> getQueuedCallWithId(String id) {
		return getHttpClient().dispatcher().queuedCalls().stream().filter(call -> isCallWithId.test(call, id))
				.findAny();
	}

	Optional<Call> getRunningCallWithId(String id) {
		return getHttpClient().dispatcher().runningCalls().stream().filter(call -> isCallWithId.test(call, id))
				.findAny();
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
				.scheme(Boolean.TRUE.equals(fireboltProperties.isSsl()) ? "https" : "http")
				.host(fireboltProperties.getHost()).port(fireboltProperties.getPort());
		parameters.forEach(httpUrlBuilder::addQueryParameter);

		pathSegments.forEach(httpUrlBuilder::addPathSegment);
		return httpUrlBuilder.build().uri();

	}

	private Map<String, String> getAllParameters(FireboltProperties fireboltProperties,
			StatementInfoWrapper statementInfoWrapper, boolean systemEngine, int queryTimeout, int maxRows,
			boolean standardSql) {
		boolean isLocalDb = PropertyUtil.isLocalDb(fireboltProperties);

		Map<String, String> params = new HashMap<>(fireboltProperties.getAdditionalProperties());

		getResponseFormatParameter(statementInfoWrapper.getType() == StatementType.QUERY, isLocalDb)
				.ifPresent(format -> params.put(format.getLeft(), format.getRight()));
		// System engines do not support the following query params
		if (!systemEngine) {
			params.put(FireboltQueryParameterKey.DATABASE.getKey(), fireboltProperties.getDatabase());
			params.put(FireboltQueryParameterKey.QUERY_ID.getKey(), statementInfoWrapper.getId());
			params.put(FireboltQueryParameterKey.COMPRESS.getKey(),
					String.format("%d", fireboltProperties.isCompress() ? 1 : 0));

			if (queryTimeout > -1) {
				params.put("max_execution_time", String.valueOf(queryTimeout));
			}
			if (maxRows > 0) {
				params.put("max_result_rows", String.valueOf(maxRows));
				params.put("result_overflow_mode", "break");
			}
		}
		if (!standardSql) {
			params.put("use_standard_sql", "0");
		}

		return params;
	}

	private Optional<Pair<String, String>> getResponseFormatParameter(boolean isQuery, boolean isLocalDb) {
		if (isQuery) {
			if (isLocalDb) {
				return Optional.of(new ImmutablePair<>(FireboltQueryParameterKey.DEFAULT_FORMAT.getKey(),
						TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT));
			} else {
				return Optional.of(new ImmutablePair<>(FireboltQueryParameterKey.OUTPUT_FORMAT.getKey(),
						TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT));
			}
		}
		return Optional.empty();
	}

	private Map<String, String> getCancelParameters(String statementId) {
		return ImmutableMap.of(FireboltQueryParameterKey.QUERY_ID.getKey(), statementId);
	}

}
