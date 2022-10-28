package com.firebolt.jdbc;

import com.firebolt.jdbc.log.FireboltLogger;
import com.firebolt.jdbc.log.JdkLogger;
import com.firebolt.jdbc.log.SLF4JLogger;
import lombok.CustomLog;
import lombok.experimental.UtilityClass;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

@UtilityClass
@CustomLog
public class LoggerUtil {

	private static boolean hasSLF4JDependency;

	static {
		try {
			Class.forName("org.slf4j.Logger");
			hasSLF4JDependency = true;
		} catch (ClassNotFoundException ex) {
			hasSLF4JDependency = false;
		}
	}

	/**
	 * Provides a {@link FireboltLogger} based on whether SLF4J is available or not.
	 * @param name logger name
	 * @return a {@link FireboltLogger}
	 */
	public static FireboltLogger getLogger(String name) {
		if (hasSLF4JDependency) {
			return new SLF4JLogger(name);
		} else {
			return new JdkLogger(name);
		}
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
			log.warn("Could not log the stream", ex);
		}
		return new ByteArrayInputStream(baos.toByteArray());
	}
}
