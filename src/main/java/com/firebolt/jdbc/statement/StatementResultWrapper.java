package com.firebolt.jdbc.statement;

import lombok.Data;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.sql.ResultSet;
import java.util.logging.Level;
import java.util.logging.Logger;

@Data
public class StatementResultWrapper implements Closeable {
	private static final Logger log = Logger.getLogger(StatementResultWrapper.class.getName());
	private ResultSet resultSet;
	private int updateCount;
	private StatementInfoWrapper statementInfoWrapper;
	private StatementResultWrapper next;

	public StatementResultWrapper(@Nullable ResultSet rs, StatementInfoWrapper statementInfoWrapper) {
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
			log.log(Level.WARNING, "Could not close ResultSet", e);
		}
		if (next != null) {
			next.close();
		}
	}

	/**
	 * Appends the result with another {@link StatementResultWrapper} This may
	 * happen if the statement executed was a multistatement returning multiple
	 * {@link ResultSet}
	 * 
	 * @param newResult the additional {@link StatementResultWrapper}
	 */
	public void append(StatementResultWrapper newResult) {
		StatementResultWrapper lastResponse = this;
		while (lastResponse.next != null) {
			lastResponse = lastResponse.next;
		}
		lastResponse.next = newResult;
	}
}
