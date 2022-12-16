package com.firebolt.jdbc;

import java.io.Closeable;
import java.io.IOException;

import lombok.CustomLog;
import lombok.experimental.UtilityClass;

@CustomLog
@UtilityClass
public class CloseableUtil {

	/**
	 * Closes the {@link Closeable} and log any potential {@link IOException}
	 * 
	 * @param closeable the closeable to close
	 */
	public void close(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (IOException e) {
				log.error("An error happened while closing the closeable: {}", e.getMessage());
			}
		}
	}
}
