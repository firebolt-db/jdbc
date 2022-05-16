package io.firebolt.jdbc.client.config;

import lombok.NoArgsConstructor;
import org.apache.http.conn.DnsResolver;
import org.apache.http.impl.conn.SystemDefaultDnsResolver;

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
}
