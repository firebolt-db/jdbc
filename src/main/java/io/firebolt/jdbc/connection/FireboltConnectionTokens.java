package io.firebolt.jdbc.connection;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class FireboltConnectionTokens {
  String accessToken;
  String refreshToken;
  long expiresInSeconds;
}
