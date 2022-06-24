package io.firebolt.jdbc.service;

import io.firebolt.jdbc.client.authentication.FireboltAuthenticationClient;
import io.firebolt.jdbc.connection.FireboltConnectionTokens;
import io.firebolt.jdbc.connection.settings.FireboltProperties;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
@Slf4j
public class FireboltAuthenticationService {

  private static final ExpiringMap<ConnectParams, FireboltConnectionTokens> tokensMap =
      ExpiringMap.builder().variableExpiration().build();
  private final FireboltAuthenticationClient fireboltAuthenticationClient;

  public FireboltConnectionTokens getConnectionTokens(String host, FireboltProperties loginProperties) {
      try {
      ConnectParams connectionParams = new ConnectParams(host, loginProperties.getUser(), loginProperties.getPassword());
      synchronized (this) {
        FireboltConnectionTokens foundToken = tokensMap.get(connectionParams);
        if (foundToken != null) {
          log.debug("Using the token of {} from the cache",host);
          return foundToken;
        } else {
          FireboltConnectionTokens fireboltConnectionTokens =
              fireboltAuthenticationClient.postConnectionTokens(host, loginProperties.getUser(), loginProperties.getPassword(), loginProperties.isCompress());
          tokensMap.put(
              connectionParams,
              fireboltConnectionTokens,
              ExpirationPolicy.CREATED,
              fireboltConnectionTokens.getExpiresInSeconds(),
              TimeUnit.SECONDS);
          return fireboltConnectionTokens;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Could not get connection tokens", e);
    }
  }

  @EqualsAndHashCode
  private static class ConnectParams {
    public final String fireboltHost;
    public final String credentialsHash;

    public ConnectParams(String fireboltHost, String user, String password)
        throws NoSuchAlgorithmException {
      this.fireboltHost = fireboltHost;
      MessageDigest md5Instance = MessageDigest.getInstance("MD5");
      Optional.ofNullable(user).map(String::getBytes).ifPresent(md5Instance::update);
      Optional.ofNullable(password).map(String::getBytes).ifPresent(md5Instance::update);
      this.credentialsHash = new String(Hex.encodeHex(md5Instance.digest()));
    }
  }
}
