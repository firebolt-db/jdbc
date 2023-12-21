package com.firebolt.jdbc.connection;

import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;

import lombok.*;

@AllArgsConstructor
@Data
@EqualsAndHashCode
public class Engine {
	private final String endpoint;
	private final String status;
	private final String name;
	private final String database;
	private final String id;
	private final List<ImmutablePair<String, String>> queryParams;
}
