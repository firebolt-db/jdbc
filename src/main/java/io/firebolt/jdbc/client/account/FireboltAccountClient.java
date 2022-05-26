package io.firebolt.jdbc.client.account;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.firebolt.jdbc.client.FireboltClient;
import io.firebolt.jdbc.client.account.response.FireboltAccountResponse;
import io.firebolt.jdbc.client.account.response.FireboltDatabaseResponse;
import io.firebolt.jdbc.client.account.response.FireboltEngineIdResponse;
import io.firebolt.jdbc.client.account.response.FireboltEngineResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ParseException;

import java.io.IOException;
import java.util.Optional;

@RequiredArgsConstructor
@Slf4j
public class FireboltAccountClient extends FireboltClient {

  private static final String GET_ACCOUNT_ID_URI = "%s/iam/v2/accounts:getIdByName?accountName=%s";

  private static final String URI_SUFFIX_ENGINE_AND_ACCOUNT_ID_BY_ENGINE_NAME =
      "engines:getIdByName?engine_name=";

  private static final String URI_SUFFIX_ACCOUNT_ENGINE_INFO_BY_ENGINE_ID = "engines/";
  private static final String URI_SUFFIX_DATABASE_INFO_URL =
      "engines:getURLByDatabaseName?databaseName=";

  private static final String URI_PREFIX_WITH_ACCOUNT_RESOURCE = "%s/core/v1/accounts/%s/%s";

  private static final String URI_PREFIX_WITHOUT_ACCOUNT_RESOURCE = "%s/core/v1/account/%s";

  private static final String ERROR_NO_RUNNING_ENGINE_SUFFIX =
      ". To connect first make sure there is a running engine and then try again.";
  private static final String ERROR_NO_RUNNING_ENGINE_PREFIX =
      "There is no running Firebolt engine running on ";

  private final CloseableHttpClient httpClient;
  private final ObjectMapper objectMapper;

  public Optional<String> getAccountId(String host, String account, String accessToken)
      throws IOException, ParseException {
    String uri = String.format(GET_ACCOUNT_ID_URI, host, account);
    return Optional.ofNullable(
            getResource(uri, accessToken, httpClient, objectMapper, FireboltAccountResponse.class))
        .map(FireboltAccountResponse::getAccountId);
  }

  public String getEngineAddress(
      String host, String accountId, String engineName, String engineID, String accessToken)
      throws IOException, ParseException {
    String uri =
        createAccountUri(accountId, host, URI_SUFFIX_ACCOUNT_ENGINE_INFO_BY_ENGINE_ID + engineID);
    FireboltEngineResponse response =
        getResource(uri, accessToken, httpClient, objectMapper, FireboltEngineResponse.class);
    return Optional.ofNullable(response)
        .map(FireboltEngineResponse::getEngine)
        .map(FireboltEngineResponse.Engine::getEndpoint)
        .orElseThrow(
            () ->
                new IOException(
                    ERROR_NO_RUNNING_ENGINE_PREFIX
                        + host
                        + " attached to "
                        + engineName
                        + ERROR_NO_RUNNING_ENGINE_SUFFIX));
  }

  public String getDbDefaultEngineAddress(
      String host, String accountId, String dbName, String accessToken)
      throws IOException, ParseException {
    String uri = createAccountUri(accountId, host, URI_SUFFIX_DATABASE_INFO_URL + dbName);
    FireboltDatabaseResponse response =
        getResource(uri, accessToken, httpClient, objectMapper, FireboltDatabaseResponse.class);
    return Optional.ofNullable(response)
        .map(FireboltDatabaseResponse::getEngineUrl)
        .orElseThrow(
            () ->
                new IOException(
                    ERROR_NO_RUNNING_ENGINE_PREFIX
                        + host
                        + " attached to "
                        + dbName
                        + ERROR_NO_RUNNING_ENGINE_SUFFIX));
  }

  public String getEngineId(String host, String accountId, String engineName, String accessToken)
      throws IOException, ParseException {
    String uri =
        createAccountUri(
            accountId, host, URI_SUFFIX_ENGINE_AND_ACCOUNT_ID_BY_ENGINE_NAME + engineName);
    FireboltEngineIdResponse response =
        getResource(uri, accessToken, httpClient, objectMapper, FireboltEngineIdResponse.class);
    return Optional.ofNullable(response)
        .map(FireboltEngineIdResponse::getEngine)
        .map(FireboltEngineIdResponse.Engine::getEngineId)
        .orElseThrow(
            () ->
                new IOException(
                    ERROR_NO_RUNNING_ENGINE_PREFIX
                        + host
                        + " with the name "
                        + engineName
                        + ERROR_NO_RUNNING_ENGINE_SUFFIX));
  }

  private String createAccountUri(String account, String host, String suffix) {
    if (StringUtils.isNotEmpty(account))
      return String.format(URI_PREFIX_WITH_ACCOUNT_RESOURCE, host, account, suffix);
    else return String.format(URI_PREFIX_WITHOUT_ACCOUNT_RESOURCE, host, suffix);
  }
}
