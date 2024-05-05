package com.firebolt.jdbc.util;

import com.firebolt.FireboltDriver;
import lombok.experimental.UtilityClass;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@UtilityClass
public class LoggerUtil {

	private static final boolean SLF4J_AVAILABLE = isSlf4jJAvailable();
	private static final Logger root = initRootLogger();
	private static final Logger log = Logger.getLogger(LoggerUtil.class.getName());

	private Logger initRootLogger() {
		Logger parent = Logger.getLogger(FireboltDriver.class.getPackageName());
		if (SLF4J_AVAILABLE) {
			synchronized (LoggerUtil.class) {
				parent.addHandler(new SLF4JBridgeHandler());
				parent.setLevel(Level.ALL);
			}
		}
		return parent;
	}

	public static Logger getRootLogger() {
		return root;
	}

	/**
	 * Logs the {@link InputStream}
	 *
	 * @param is the {@link InputStream}
	 * @return a copy of the {@link InputStream} provided
	 */
	public InputStream logInputStream(InputStream is) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			byte[] buffer = new byte[1024];
			int len;
			while ((len = is.read(buffer)) > -1) {
				baos.write(buffer, 0, len);
			}
			baos.flush();
			InputStream streamToLog = new ByteArrayInputStream(baos.toByteArray());
			String text = new BufferedReader(new InputStreamReader(streamToLog, StandardCharsets.UTF_8)).lines()
					.collect(Collectors.joining("\n"));
			log.info("======================================");
			log.info(text);
			log.info("======================================");
			return new ByteArrayInputStream(baos.toByteArray());
		} catch (Exception ex) {
			log.log(Level.WARNING, "Could not log the stream", ex);
		}
		return new ByteArrayInputStream(baos.toByteArray());
	}

	private static boolean isSlf4jJAvailable() {
		try {
			Class.forName("org.slf4j.Logger");
			return true;
		} catch (ClassNotFoundException ex) {
			return false;
		}
	}
}
