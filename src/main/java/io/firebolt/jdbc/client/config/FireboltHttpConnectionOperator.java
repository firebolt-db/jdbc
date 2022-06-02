package io.firebolt.jdbc.client.config;

import io.firebolt.jdbc.connection.settings.FireboltProperties;
import jdk.net.ExtendedSocketOptions;
import jdk.net.Sockets;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.*;
import org.apache.hc.client5.http.impl.ConnPoolSupport;
import org.apache.hc.client5.http.impl.DefaultSchemePortResolver;
import org.apache.hc.client5.http.impl.io.DefaultHttpClientConnectionOperator;
import org.apache.hc.client5.http.io.ManagedHttpClientConnection;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.core5.annotation.Contract;
import org.apache.hc.core5.annotation.Internal;
import org.apache.hc.core5.annotation.ThreadingBehavior;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.config.Lookup;
import org.apache.hc.core5.http.io.SocketConfig;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.util.Args;
import org.apache.hc.core5.util.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

@Slf4j
@Internal
@Contract(threading = ThreadingBehavior.STATELESS)
public class FireboltHttpConnectionOperator extends DefaultHttpClientConnectionOperator {

  static final String SOCKET_FACTORY_REGISTRY = "http.socket-factory-registry";

  private static final Logger LOG =
      LoggerFactory.getLogger(
          org.apache.hc.client5.http.impl.io.DefaultHttpClientConnectionOperator.class);

  private final Lookup<ConnectionSocketFactory> socketFactoryRegistry;
  private final SchemePortResolver schemePortResolver;
  private final DnsResolver dnsResolver;

  private final FireboltProperties fireboltProperties;

  public FireboltHttpConnectionOperator(
      final Lookup<ConnectionSocketFactory> socketFactoryRegistry,
      final SchemePortResolver schemePortResolver,
      final DnsResolver dnsResolver,
      final FireboltProperties fireboltProperties) {
    super(socketFactoryRegistry, schemePortResolver, dnsResolver);
    Args.notNull(socketFactoryRegistry, "Socket factory registry");
    this.socketFactoryRegistry = socketFactoryRegistry;
    this.schemePortResolver =
        schemePortResolver != null ? schemePortResolver : DefaultSchemePortResolver.INSTANCE;
    this.dnsResolver = dnsResolver != null ? dnsResolver : SystemDefaultDnsResolver.INSTANCE;
    this.fireboltProperties = fireboltProperties;
  }

  @SuppressWarnings("unchecked")
  private Lookup<ConnectionSocketFactory> getSocketFactoryRegistry(final HttpContext context) {
    Lookup<ConnectionSocketFactory> reg =
        (Lookup<ConnectionSocketFactory>) context.getAttribute(SOCKET_FACTORY_REGISTRY);
    if (reg == null) {
      reg = this.socketFactoryRegistry;
    }
    return reg;
  }

  @Override
  public void connect(
      final ManagedHttpClientConnection conn,
      final HttpHost host,
      final InetSocketAddress localAddress,
      final TimeValue connectTimeout,
      final SocketConfig socketConfig,
      final HttpContext context)
      throws IOException {
    Args.notNull(conn, "Connection");
    Args.notNull(host, "Host");
    Args.notNull(socketConfig, "Socket config");
    Args.notNull(context, "Context");
    final Lookup<ConnectionSocketFactory> registry = getSocketFactoryRegistry(context);
    final ConnectionSocketFactory sf = registry.lookup(host.getSchemeName());
    if (sf == null) {
      throw new UnsupportedSchemeException(host.getSchemeName() + " protocol is not supported");
    }
    final InetAddress[] addresses =
        host.getAddress() != null
            ? new InetAddress[] {host.getAddress()}
            : this.dnsResolver.resolve(host.getHostName());
    final int port = this.schemePortResolver.resolve(host);
    for (int i = 0; i < addresses.length; i++) {
      final InetAddress address = addresses[i];
      final boolean last = i == addresses.length - 1;

      Socket sock = sf.createSocket(context);
      sock.setSoTimeout(socketConfig.getSoTimeout().toMillisecondsIntBound());
      sock.setReuseAddress(socketConfig.isSoReuseAddress());
      sock.setTcpNoDelay(socketConfig.isTcpNoDelay());
      sock.setKeepAlive(socketConfig.isSoKeepAlive());
      if (socketConfig.getRcvBufSize() > 0) {
        sock.setReceiveBufferSize(socketConfig.getRcvBufSize());
      }
      if (socketConfig.getSndBufSize() > 0) {
        sock.setSendBufferSize(socketConfig.getSndBufSize());
      }

      Sockets.setOption(sock, ExtendedSocketOptions.TCP_KEEPIDLE, fireboltProperties.getTcpKeepIdle());
      Sockets.setOption(sock, ExtendedSocketOptions.TCP_KEEPCOUNT, fireboltProperties.getTcpKeepCount());
      Sockets.setOption(sock, ExtendedSocketOptions.TCP_KEEPINTERVAL, fireboltProperties.getTcpKeepInterval());

      final int linger = socketConfig.getSoLinger().toMillisecondsIntBound();
      if (linger >= 0) {
        sock.setSoLinger(true, linger);
      }
      conn.bind(sock);

      final InetSocketAddress remoteAddress = new InetSocketAddress(address, port);
      log.debug("{} connecting to {}", ConnPoolSupport.getId(conn), remoteAddress);
      try {
        sock = sf.connectSocket(connectTimeout, sock, host, remoteAddress, localAddress, context);
        conn.bind(sock);
        log.debug("{} connection established {}", ConnPoolSupport.getId(conn), conn);

        return;
      } catch (final IOException ex) {
        if (last) {
          throw ConnectExceptionSupport.enhance(ex, host, addresses);
        }
      }
      log.debug(
          "{} connect to {} timed out. Connection will be retried using another IP address",
          ConnPoolSupport.getId(conn),
          remoteAddress);
    }
  }
}
