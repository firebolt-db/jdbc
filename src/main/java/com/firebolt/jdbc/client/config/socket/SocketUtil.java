package com.firebolt.jdbc.client.config.socket;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketOption;

import com.firebolt.jdbc.connection.settings.FireboltProperties;

import jdk.net.ExtendedSocketOptions;
import jdk.net.Sockets;
import lombok.CustomLog;
import lombok.experimental.UtilityClass;

@UtilityClass
@CustomLog
public class SocketUtil {

	public static Socket wrap(Socket s, FireboltProperties fireboltProperties) throws IOException {
		s.setKeepAlive(true);
		s.setTcpNoDelay(true);
		try {
			setSocketOption(s, ExtendedSocketOptions.TCP_KEEPIDLE, fireboltProperties.getTcpKeepIdle());
			setSocketOption(s, ExtendedSocketOptions.TCP_KEEPCOUNT, fireboltProperties.getTcpKeepCount());
			setSocketOption(s, ExtendedSocketOptions.TCP_KEEPINTERVAL, fireboltProperties.getTcpKeepInterval());
		} catch (Error | Exception e) {
			log.debug("Could not set socket options", e);
		}
		return s;
	}

	private void setSocketOption(Socket socket, SocketOption<Integer> option, int value) throws IOException {
		try {
			Sockets.setOption(socket, option, value);
		} catch (Exception e) {
			log.debug("Could not set the socket option {}. The operation is not supported: {}", option.name(),
					e.getMessage());
		}
	}
}
