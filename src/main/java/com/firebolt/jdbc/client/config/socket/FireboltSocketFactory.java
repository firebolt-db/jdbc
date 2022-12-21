package com.firebolt.jdbc.client.config.socket;

import static com.firebolt.jdbc.client.config.socket.SocketUtil.wrap;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import javax.net.SocketFactory;

import com.firebolt.jdbc.connection.settings.FireboltProperties;

import lombok.CustomLog;

@CustomLog
public class FireboltSocketFactory extends SocketFactory {
	private static final javax.net.SocketFactory delegate = SocketFactory.getDefault();
	private final FireboltProperties fireboltProperties;

	public FireboltSocketFactory(FireboltProperties fireboltProperties) {
		this.fireboltProperties = fireboltProperties;
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
	public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort)
			throws IOException {
		return wrap(delegate.createSocket(address, port, localAddress, localPort), fireboltProperties);
	}

}
