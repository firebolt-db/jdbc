package com.firebolt.jdbc.client.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.firebolt.jdbc.CloseableUtil;
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
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;

import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;

@Slf4j
public class StatementClientImpl extends FireboltClient implements StatementClient {


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
     * @param queryParams          the statement parameters
     * @return the server response
     */
    @Override
    public InputStream postSqlStatement(@NonNull StatementInfoWrapper statementInfoWrapper,
                                        @NonNull FireboltProperties connectionProperties, Map<String, String> queryParams)
            throws FireboltException {
        String formattedStatement = formatStatement(statementInfoWrapper);

        try {
            String uri = this.buildQueryUri(connectionProperties, queryParams).toString();
            Request post = this.createHttpPost(uri, this.getConnection().getConnectionTokens()
                    .map(FireboltConnectionTokens::getAccessToken).orElse(null), formattedStatement, statementInfoWrapper.getId());
            log.debug("Posting statement with id {} to URI: {}", statementInfoWrapper.getId(), uri);
            Response response = this.execute(post, connectionProperties.getHost(),
                    connectionProperties.isCompress());
            return response.body().byteStream();
        } catch (FireboltException e) {
            throw e;
        } catch (Exception e) {
            String errorMessage = String.format("Error executing statement with id %s: %s",
                    statementInfoWrapper.getId(), formattedStatement);
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
     * @param queryParams        query parameters
     */
    public void abortStatement(String id, FireboltProperties fireboltProperties, Map<String, String> queryParams)
            throws FireboltException {
        try {

            String uri = this.buildCancelUri(fireboltProperties, queryParams).toString();
            Request rq = this.createHttpPost(uri, this.getConnection().getConnectionTokens()
                    .map(FireboltConnectionTokens::getAccessToken).orElse(null), null, null);
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
        return getHttpClient().dispatcher().queuedCalls().stream()
                .filter(call -> isCallWithId.test(call, id)).findAny();
    }

    Optional<Call> getRunningCallWithId(String id) {
        return getHttpClient().dispatcher().runningCalls().stream()
                .filter(call -> isCallWithId.test(call, id)).findAny();
    }

    @Override
    public boolean isStatementRunning(String statementId) {
        return getQueuedCallWithId(statementId).isPresent() || getRunningCallWithId(statementId).isPresent();
    }

    private URI buildQueryUri(FireboltProperties fireboltProperties, Map<String, String> parameters) {
        return buildURI(fireboltProperties, parameters, Collections.emptyList());
    }

    private URI buildCancelUri(FireboltProperties fireboltProperties, Map<String, String> parameters) {
        return buildURI(fireboltProperties, parameters, Collections.singletonList("cancel"));
    }

    private URI buildURI(FireboltProperties fireboltProperties, Map<String, String> parameters, List<String> pathSegments) {
        HttpUrl.Builder httpUrlBuilder = new HttpUrl.Builder()
                .scheme(Boolean.TRUE.equals(fireboltProperties.isSsl()) ? "https" : "http")
                .fragment("/")
                .host(fireboltProperties.getHost())
                .port(fireboltProperties.getPort());
        parameters.forEach(httpUrlBuilder::addQueryParameter);

        pathSegments.forEach(httpUrlBuilder::addPathSegment);
        return httpUrlBuilder.build().uri();


    }

}
