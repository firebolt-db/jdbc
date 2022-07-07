package io.firebolt.jdbc.service;

import com.google.common.collect.ImmutableMap;
import io.firebolt.jdbc.statement.StatementInfoWrapper;
import io.firebolt.jdbc.client.query.StatementClient;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import io.firebolt.jdbc.exception.FireboltException;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@RequiredArgsConstructor
@Slf4j
public class FireboltStatementService {

  private final StatementClient statementClient;

  private static final String TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT =
      "TabSeparatedWithNamesAndTypes";

  public InputStream execute(
      @NonNull StatementInfoWrapper statementInfoWrapper,
      @NonNull FireboltProperties connectionProperties,
      Map<String, String> statementParams)
      throws FireboltException {
    Map<String, String> params =
        getAllParameters(connectionProperties, statementInfoWrapper, statementParams);
    return statementClient.postSqlStatement(statementInfoWrapper, connectionProperties, params);
  }

  public void cancel(@NonNull String statementId, @NonNull FireboltProperties properties)
      throws FireboltException {
    log.debug("Cancelling statement with id: {}", statementId);
    statementClient.postCancelSqlStatement(statementId, properties, getCancelParameters(statementId));
  }

  private Map<String, String> getCancelParameters(String statementId) {
    return ImmutableMap.of("query_id", statementId);
  }

  private Map<String, String> getAllParameters(
      FireboltProperties fireboltProperties,
      StatementInfoWrapper statementInfoWrapper,
      Map<String, String> statementParams) {
    boolean isLocalDb = StringUtils.equalsIgnoreCase("localhost", fireboltProperties.getHost());

    Map<String, String> params = new HashMap<>(fireboltProperties.getAdditionalProperties());

    getResponseFormatParameter(statementInfoWrapper.isQuery(), isLocalDb)
        .ifPresent(format -> params.put(format.getLeft(), format.getRight()));

    params.put("database", fireboltProperties.getDatabase());
    params.put("query_id", statementInfoWrapper.getId());
    params.put("compress", String.format("%s", fireboltProperties.isCompress()));
    Optional.ofNullable(statementParams).ifPresent(params::putAll);
    return params;
  }

  private Optional<Pair<String, String>> getResponseFormatParameter(
      boolean isQuery, boolean isLocalDb) {
    if (isQuery) {
      if (isLocalDb) {
        return Optional.of(
            new ImmutablePair<>("default_format", TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT));
      } else {
        return Optional.of(
            new ImmutablePair<>("output_format", TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT));
      }
    }
    return Optional.empty();
  }
}
