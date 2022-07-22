/*
 * Copyright 2022 Firebolt Analytics, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * NOTICE: THIS FILE HAS BEEN MODIFIED BY Firebolt Analytics, Inc. UNDER COMPLIANCE WITH THE APACHE 2.0 LICENCE FROM THE ORIGINAL WORK
OF the Apache Software Foundation (ASF)
 * Changes:
 *  - Class and file name
 *  - Imports
 *  - Package name
 *  - Formatting
 *  - Socket options setting
 *  - Logging
 *  - Replace null check with lombok annotation @NonNull
 *
 */

/*
 * ====================================================================
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 */
package com.firebolt.jdbc.client.config;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import jdk.net.ExtendedSocketOptions;
import jdk.net.Sockets;
import lombok.NonNull;
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
import org.apache.hc.core5.util.TimeValue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketOption;

@Slf4j
@Internal
@Contract(threading = ThreadingBehavior.STATELESS)
public class FireboltHttpConnectionOperator extends DefaultHttpClientConnectionOperator {

  static final String SOCKET_FACTORY_REGISTRY = "http.socket-factory-registry";

  private final Lookup<ConnectionSocketFactory> socketFactoryRegistry;
  private final SchemePortResolver schemePortResolver;
  private final DnsResolver dnsResolver;

  private final FireboltProperties fireboltProperties;

  public FireboltHttpConnectionOperator(
      @NonNull final Lookup<ConnectionSocketFactory> socketFactoryRegistry,
      final SchemePortResolver schemePortResolver,
      final DnsResolver dnsResolver,
      final FireboltProperties fireboltProperties) {
    super(socketFactoryRegistry, schemePortResolver, dnsResolver);
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
      @NonNull final ManagedHttpClientConnection conn,
      @NonNull final HttpHost host,
      final InetSocketAddress localAddress,
      final TimeValue connectTimeout,
      @NonNull final SocketConfig socketConfig,
      @NonNull final HttpContext context)
      throws IOException {
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

      this.setSocketOption(
          sock, ExtendedSocketOptions.TCP_KEEPIDLE, fireboltProperties.getTcpKeepIdle());
      this.setSocketOption(
          sock, ExtendedSocketOptions.TCP_KEEPCOUNT, fireboltProperties.getTcpKeepCount());
      this.setSocketOption(
          sock, ExtendedSocketOptions.TCP_KEEPINTERVAL, fireboltProperties.getTcpKeepInterval());

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

  private void setSocketOption(Socket socket, SocketOption<Integer> option, int value)
      throws IOException {
    try {
      Sockets.setOption(socket, option, value);
    } catch (UnsupportedOperationException e) {
      log.debug(
          "Could not set the socket option {}. The operation is not supported: {}",
          option.name(),
          e.getMessage());
    }
  }
}
