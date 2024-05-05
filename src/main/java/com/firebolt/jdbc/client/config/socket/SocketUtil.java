package com.firebolt.jdbc.client.config.socket;

import com.firebolt.jdbc.connection.settings.FireboltProperties;
import jdk.net.ExtendedSocketOptions;
import jdk.net.Sockets;
import lombok.experimental.UtilityClass;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketOption;
import java.util.logging.Level;
import java.util.logging.Logger;

@UtilityClass
public class SocketUtil {
	private static final Logger log = Logger.getLogger(SocketUtil.class.getName());

	public static Socket wrap(Socket s, FireboltProperties fireboltProperties) throws IOException {
		s.setKeepAlive(true);
		s.setTcpNoDelay(true);
		try {
			setSocketOption(s, ExtendedSocketOptions.TCP_KEEPIDLE, fireboltProperties.getTcpKeepIdle());
			setSocketOption(s, ExtendedSocketOptions.TCP_KEEPCOUNT, fireboltProperties.getTcpKeepCount());
			setSocketOption(s, ExtendedSocketOptions.TCP_KEEPINTERVAL, fireboltProperties.getTcpKeepInterval());
		} catch (Error | Exception e) {
			log.log(Level.FINE, "Could not set socket options", e);
		}
		return s;
	}

	private void setSocketOption(Socket socket, SocketOption<Integer> option, int value) throws IOException {
		try {
			Sockets.setOption(socket, option, value);
		} catch (Exception e) {
			log.log(Level.FINE, "Could not set the socket option {0}. The operation is not supported: {1}", new Object[] {option.name(), e.getMessage()});
		}
	}
}
