package com.firebolt.jdbc.service;

import static com.firebolt.jdbc.exception.ExceptionType.INVALID_REQUEST;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.firebolt.jdbc.PropertyUtil;
import com.firebolt.jdbc.client.query.StatementClient;
import com.firebolt.jdbc.connection.settings.FireboltProperties;
import com.firebolt.jdbc.connection.settings.FireboltQueryParameterKey;
import com.firebolt.jdbc.exception.FireboltException;
import com.firebolt.jdbc.statement.StatementInfoWrapper;
import com.firebolt.jdbc.statement.StatementType;
import com.google.common.collect.ImmutableMap;

import lombok.CustomLog;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@CustomLog
public class FireboltStatementService {

	private static final String TAB_SEPARATED_WITH_NAMES_AND_TYPES_FORMAT = "TabSeparatedWithNamesAndTypes";
	private final StatementClient statementClient;

	private final boolean systemEngine;

	public InputStream execute(@NonNull StatementInfoWrapper statementInfoWrapper,
			@NonNull FireboltProperties connectionProperties, Map<String, String> statementParams)
			throws FireboltException {
		Map<String, String> params = getAllParameters(connectionProperties, statementInfoWrapper, statementParams);
		return statementClient.postSqlStatement(statementInfoWrapper, connectionProperties, params);
	}

	public void abortStatement(@NonNull String statementId, @NonNull FireboltProperties properties)
			throws FireboltException {
		if (systemEngine) {
			throw new FireboltException("Cannot cancel a statement using a system engine", INVALID_REQUEST);
		} else {
			statementClient.abortStatement(statementId, properties, getCancelParameters(statementId));
		}
	}

	public void abortStatementHttpRequest(@NonNull String statementId) throws FireboltException {
		statementClient.abortRunningHttpRequest(statementId);
	}

	private Map<String, String> getCancelParameters(String statementId) {
		return ImmutableMap.of(FireboltQueryParameterKey.QUERY_ID.getKey(), statementId);
	}

	private Map<String, String> getAllParameters(FireboltProperties fireboltProperties,
			StatementInfoWrapper statementInfoWrapper, Map<String, String> statementParams) {
		boolean isLocalDb = PropertyUtil.isLocalDb(fireboltProperties);

		Map<String, String> params = new HashMap<>(fireboltProperties.getAdditionalProperties());

		getResponseFormatParameter(statementInfoWrapper.getType() == StatementType.QUERY, isLocalDb)
				.ifPresent(format -> params.put(format.getLeft(), format.getRight()));

		//System engines do not support the following query params
		if (!systemEngine) {
			params.put(FireboltQueryParameterKey.DATABASE.getKey(), fireboltProperties.getDatabase());
			params.put(FireboltQueryParameterKey.QUERY_ID.getKey(), statementInfoWrapper.getId());
			params.put(FireboltQueryParameterKey.COMPRESS.getKey(),
					String.format("%d", fireboltProperties.isCompress() ? 1 : 0));
		}
		Optional.ofNullable(statementParams).ifPresent(params::putAll);
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

	public boolean isStatementRunning(String statementId) {
		return statementClient.isStatementRunning(statementId);
	}
}
