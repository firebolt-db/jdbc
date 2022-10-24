package com.firebolt.jdbc.client.config;

import lombok.NoArgsConstructor;
import okhttp3.Dns;
import org.jetbrains.annotations.NotNull;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.stream.Collectors;

@NoArgsConstructor
public class DnsResolverByIpVersionPriority implements Dns {

    @NotNull
    @Override
    public List<InetAddress> lookup(@NotNull String s) throws UnknownHostException {
        List<InetAddress> addresses = Dns.SYSTEM.lookup(s);
        return addresses.stream().sorted((address1, address2) -> {
            int address1IPV6 = address1 instanceof Inet6Address ? 1 : 0;
            int address2IPV6 = address2 instanceof Inet6Address ? 1 : 0;
            return address2IPV6 - address1IPV6;
        }).collect(Collectors.toList());
    }
}
