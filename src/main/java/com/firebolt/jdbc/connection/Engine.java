package com.firebolt.jdbc.connection;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class Engine {
	private String endpoint;
	private String id;
	private String status;
	private String name;
}
