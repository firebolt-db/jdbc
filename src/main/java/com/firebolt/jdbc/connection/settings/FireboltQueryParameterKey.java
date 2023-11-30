package com.firebolt.jdbc.connection.settings;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public enum FireboltQueryParameterKey {
	DATABASE("database"),
	QUERY_ID("query_id"),
	QUERY_LABEL("query_label"),
	COMPRESS("compress"),
	DEFAULT_FORMAT("default_format"),
	OUTPUT_FORMAT("output_format"),
	ACCOUNT_ID("account_id"),
	;

	private final String key;
}