package com.firebolt.jdbc;

import java.io.Closeable;
import java.io.IOException;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class CloseableUtil {

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
