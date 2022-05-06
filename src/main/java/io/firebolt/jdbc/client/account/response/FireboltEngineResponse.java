package io.firebolt.jdbc.client.account.response;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class FireboltEngineResponse {
  Engine engine;

  @Value
  @Builder
  public static class Engine {
    String endpoint;
  }
}
