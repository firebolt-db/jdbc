package io.firebolt.jdbc.client.config;

import lombok.NoArgsConstructor;
import org.apache.hc.client5.http.DnsResolver;
import org.apache.hc.client5.http.SystemDefaultDnsResolver;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

@NoArgsConstructor
public class DnsResolverByIpVersionPriority implements DnsResolver {

  private final DnsResolver defaultResolver = new SystemDefaultDnsResolver();

  @Override
  public InetAddress[] resolve(String host) throws UnknownHostException {
    InetAddress[] ips = defaultResolver.resolve(host);

    // Sort ip addresses by version, from IPV6 to IPV4
    return Arrays.stream(ips)
        .sorted(
            (address1, address2) -> {
              int address1IPV6 = address1 instanceof Inet6Address ? 1 : 0;
              int address2IPV6 = address2 instanceof Inet6Address ? 1 : 0;
              return address2IPV6 - address1IPV6;
            })
        .toArray(InetAddress[]::new);
  }

  /**
   * Implementation based on {@link SystemDefaultDnsResolver#resolveCanonicalHostname(String)}
   * except that it picks the ip using IPV6 in priority.
   */
  @Override
  public String resolveCanonicalHostname(String host) throws UnknownHostException {
    if (host == null) {
      return null;
    }

    InetAddress ip = this.resolve(host)[0];
    final String canonicalServer = ip.getCanonicalHostName();
    if (ip.getHostAddress().contentEquals(canonicalServer)) {
      return host;
    }
    return canonicalServer;
  }
}
