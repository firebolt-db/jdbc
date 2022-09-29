package com.firebolt.jdbc;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import lombok.experimental.UtilityClass;

@UtilityClass
public class LoggerUtil {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(LoggerUtil.class);

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
			log.warn("Could not log stream received", ex);
		}
		return new ByteArrayInputStream(baos.toByteArray());
	}
}
