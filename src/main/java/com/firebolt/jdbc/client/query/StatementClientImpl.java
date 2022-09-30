package com.firebolt.jdbc.client.query;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.RequestFailedException;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.hc.core5.net.URIBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.client.FireboltClient;
import com.firebolt.jdbc.connection.FireboltConnection;
import com.firebolt.jdbc.connection.FireboltConnectionTokens;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.exception.ExceptionType;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.rawstatement.RawStatement;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StatementClientImpl extends FireboltClient implements StatementClient {
	private final Map<String, HttpPost> runningStatements = new HashMap<>();

	public StatementClientImpl(CloseableHttpClient httpClient, FireboltConnection connection, ObjectMapper objectMapper,
			String customDrivers, String customClients) {
		super(httpClient, connection, customDrivers, customClients, objectMapper);
	}

	/**
	 * Sends SQL statement to Firebolt
	 * 
	 * @param statementInfoWrapper the statement wrapper
	 * @param connectionProperties the connection properties
	 * @param queryParams          the statement parameters
	 * @return the server response
	 */
	@Override
	public InputStream postSqlStatement(@NonNull StatementInfoWrapper statementInfoWrapper,
			@NonNull FireboltProperties connectionProperties, Map<String, String> queryParams)
			throws FireboltException {
		String formattedStatement = formatStatement(statementInfoWrapper);

		try (StringEntity entity = new StringEntity(formattedStatement, StandardCharsets.UTF_8)) {
			List<NameValuePair> parameters = queryParams.entrySet().stream()
					.map(e -> new BasicNameValuePair(e.getKey(), e.getValue())).collect(Collectors.toList());
			String uri = this.buildQueryUri(connectionProperties, parameters).toString();
			HttpPost post = this.createPostRequest(uri, this.getConnection().getConnectionTokens()
					.map(FireboltConnectionTokens::getAccessToken).orElse(null));
			post.setEntity(entity);
			log.debug("Posting statement with id {} to URI: {}", statementInfoWrapper.getId(), uri);
			runningStatements.put(statementInfoWrapper.getId(), post);
			CloseableHttpResponse response = this.execute(post, connectionProperties.getHost(),
					connectionProperties.isCompress());
			return response.getEntity().getContent();
		} catch (FireboltException e) {
			throw e;
		} catch (Exception e) {
			String errorMessage = String.format("Error executing statement with id %s: %s",
					statementInfoWrapper.getId(), formattedStatement);
			if (e instanceof RequestFailedException) {
				throw new FireboltException(errorMessage, e, ExceptionType.REQUEST_FAILED);
			} else {
				throw new FireboltException(errorMessage, e);
			}

		} finally {
			runningStatements.remove(statementInfoWrapper.getId());
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
	 * @param queryParams        query parameters
	 */
	public void abortStatement(String id, FireboltProperties fireboltProperties, Map<String, String> queryParams)
			throws FireboltException {
		try {
			List<NameValuePair> params = queryParams.entrySet().stream()
					.map(kv -> new BasicNameValuePair(kv.getKey(), kv.getValue())).collect(Collectors.toList());

			String uri = this.buildCancelUri(fireboltProperties, params).toString();
			HttpPost post = this.createPostRequest(uri, this.getConnection().getConnectionTokens()
					.map(FireboltConnectionTokens::getAccessToken).orElse(null));
			try (CloseableHttpResponse response = this.execute(post, fireboltProperties.getHost())) {
				EntityUtils.consumeQuietly(response.getEntity());
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
		HttpPost running = runningStatements.get(id);
		if (running != null) {
			running.abort();
		}
	}

	@Override
	public boolean isStatementRunning(String statementId) {
		return runningStatements.containsKey(statementId);
	}

	private URI buildQueryUri(FireboltProperties fireboltProperties, List<NameValuePair> parameters)
			throws URISyntaxException {

		return new URIBuilder().setScheme(Boolean.TRUE.equals(fireboltProperties.isSsl()) ? "https" : "http")
				.setHost(fireboltProperties.getHost()).setPort(fireboltProperties.getPort()).setPath("/")
				.setParameters(parameters).build();
	}

	private URI buildCancelUri(FireboltProperties fireboltProperties, List<NameValuePair> parameters)
			throws URISyntaxException {

		return new URIBuilder().setScheme(Boolean.TRUE.equals(fireboltProperties.isSsl()) ? "https" : "http")
				.setHost(fireboltProperties.getHost()).setPort(fireboltProperties.getPort()).setPath("/cancel")
				.setParameters(parameters).build();
	}
}
