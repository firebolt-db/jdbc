package com.firebolt.jdbc.client.config.socket;


import com.firebolt.jdbc.connection.settings.FireboltProperties;
import jdk.net.ExtendedSocketOptions;
import jdk.net.Sockets;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketOption;

@UtilityClass
@Slf4j
public class SocketUtil {

    public static Socket wrap(Socket s, FireboltProperties fireboltProperties) throws IOException {
        s.setReceiveBufferSize(fireboltProperties.getClientBufferSize());
        s.setKeepAlive(true);
        s.setTcpNoDelay(true);
        setSocketOption(s, ExtendedSocketOptions.TCP_KEEPIDLE, fireboltProperties.getTcpKeepIdle());
        setSocketOption(s, ExtendedSocketOptions.TCP_KEEPCOUNT, fireboltProperties.getTcpKeepCount());
        setSocketOption(s, ExtendedSocketOptions.TCP_KEEPINTERVAL, fireboltProperties.getTcpKeepInterval());
        return s;
    }

    private void setSocketOption(Socket socket, SocketOption<Integer> option, int value) throws IOException {
        try {
            Sockets.setOption(socket, option, value);
        } catch (UnsupportedOperationException e) {
            log.debug("Could not set the socket option {}. The operation is not supported: {}", option.name(),
                    e.getMessage());
        }
    }
}
