package com.firebolt.jdbc.statement;

import java.io.Closeable;
import java.sql.ResultSet;

import org.checkerframework.checker.nullness.qual.Nullable;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class StatementResponseWrapper implements Closeable {
	ResultSet resultSet;
	int updateCount;
	StatementInfoWrapper statementInfoWrapper;
	StatementResponseWrapper next;

	public StatementResponseWrapper(@Nullable ResultSet rs, StatementInfoWrapper statementInfoWrapper) {
		this.resultSet = rs;
		this.updateCount = -1;
		this.statementInfoWrapper = statementInfoWrapper;
	}

	@Override
	public void close() {
		try {
			if (resultSet != null) {
				resultSet.close();
			}
		} catch (Exception e) {
			log.warn("Could not close ResultSet", e);
		}
		if (next != null) {
			next.close();
		}
	}

	public void append(StatementResponseWrapper newResult) {
		StatementResponseWrapper lastResponse = this;
		while (lastResponse.next != null) {
			lastResponse = lastResponse.next;
		}
		lastResponse.next = newResult;
	}
}
