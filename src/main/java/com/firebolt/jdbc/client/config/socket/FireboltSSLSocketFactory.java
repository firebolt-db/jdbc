package com.firebolt.jdbc.client.config.socket;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import lombok.extern.slf4j.Slf4j;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import static com.firebolt.jdbc.client.config.socket.SocketUtil.wrap;

@Slf4j
public class FireboltSSLSocketFactory extends SSLSocketFactory {
    private final SSLSocketFactory delegate;
    private final FireboltProperties fireboltProperties;

    public FireboltSSLSocketFactory(FireboltProperties fireboltProperties, SSLSocketFactory sslSocketFactory) {
        this.fireboltProperties = fireboltProperties;
        delegate = sslSocketFactory;
    }


    @Override
    public Socket createSocket() throws IOException {
        return wrap(delegate.createSocket(), fireboltProperties);
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        return wrap(delegate.createSocket(host, port), fireboltProperties);
    }

    @Override
    public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
        return wrap(delegate.createSocket(host, port, localHost, localPort), fireboltProperties);
    }

    @Override
    public Socket createSocket(InetAddress host, int port) throws IOException {
        return wrap(delegate.createSocket(host, port), fireboltProperties);
    }

    @Override
    public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
        return wrap(delegate.createSocket(address, port, localAddress, localPort), fireboltProperties);
    }

    @Override
    public String[] getDefaultCipherSuites() {
        return delegate.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
        return delegate.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket(Socket s, String host, int port, boolean autoClose) throws IOException {
        return delegate.createSocket(s, host, port, autoClose);
    }
}
