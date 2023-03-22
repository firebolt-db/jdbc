package com.firebolt.jdbc.connection;

import lombok.*;

@AllArgsConstructor
@Data
@EqualsAndHashCode
public class Engine {
	private final String endpoint;
	private final String status;
	private final String name;
	private final String database;
}
