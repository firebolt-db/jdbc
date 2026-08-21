# REPRO_RESULTS: InputStreamUtil.readAllBytes infinite loop

## Summary

`InputStreamUtil.readAllBytes()` (introduced in PR [#216](https://github.com/firebolt-db/jdbc/pull/216)) drains non-query HTTP response bodies in a `while (true)` loop that **catches `IOException` and only logs a WARN**. When the underlying stream is permanently broken (e.g. HTTP/2 `StreamResetException: stream was reset: CANCEL`), every `read()` throws immediately, the loop never exits, `executeUpdate()` hangs forever, and the same thread spam-logs WARNs at extremely high frequency.

**Call site:** `FireboltStatementService.executeStatementInternal()` → `InputStreamUtil.readAllBytes(is)`.

## Driver versions tested

| Version | Result |
|--------|--------|
| `v3.7.1` (tag) | Same buggy `while (true)` + catch/`log.warn` implementation present (source inspection via `git show v3.7.1:.../InputStreamUtil.java`) |
| `main` / `3.10.2` (this branch base `625a1ee`) | Bug still present before the fix on this branch |
| This branch after fix | Stage 1–2 pass; full `./gradlew test` green |

## Stage 1 — Unit-level proof (no network)

**Method:** `InputStream` stub whose `read()` always throws `IOException("stream was reset: CANCEL")`, exercised via `assertTimeoutPreemptively(10s)` and a watchdog thread stack capture. Hang capture enabled with `REPRO_INPUTSTREAM_HANG=true`.

### Before fix (main / 3.10.2)

- Watchdog thread still alive after 2s inside `InputStreamUtil.readAllBytes`.
- `assertTimeoutPreemptively(Duration.ofSeconds(10), …)` failed with: `execution timed out after 10000 ms`.
- Captured stack (excerpt):

```
app//ch.qos.logback.classic.Logger.warn(...)
app//com.firebolt.jdbc.log.SLF4JLogger.warn(SLF4JLogger.java:72)
app//com.firebolt.jdbc.util.InputStreamUtil.readAllBytes(InputStreamUtil.java:28)
app//com.firebolt.jdbc.util.InputStreamUtilInfiniteLoopReproTest.lambda$captureHangStackWhenReproFlagSet$2(...)
java.base@17.0.20.1/java.lang.Thread.run(Thread.java:840)
```

### After fix

- `readAllBytesPropagatesIoExceptionInsteadOfHanging` completes instantly; `IOException` is thrown out of `readAllBytes`.
- Hang-capture flag no longer observes a stuck thread (expected).

## Stage 2 — Deterministic HTTP/2 MockWebServer repro (no credentials)

**Method:** OkHttp `MockWebServer` with `Protocol.H2_PRIOR_KNOWLEDGE`. Custom duplex response writes a partial body then `stream.close(ErrorCode.CANCEL, null)`. `StatementClientImpl` + `FireboltStatementService` + `FireboltStatement.executeUpdate("INSERT …")` on a watchdog thread. Hang capture: `REPRO_HTTP2_HANG=true` (file appenders detached to avoid JVM crash from WARN flood).

### Before fix

- `executeUpdate` never returned within the observation window.
- Stack showed the real driver path + HTTP/2 framing source:

```
app//okhttp3.internal.http2.Http2Stream$FramingSource.read(Http2Stream.kt:350)
app//okhttp3.internal.connection.Exchange$ResponseBodySource.read(Exchange.kt:281)
app//okio.RealBufferedSource$inputStream$1.read(RealBufferedSource.kt:150)
app//com.firebolt.jdbc.util.InputStreamUtil.readAllBytes(InputStreamUtil.java:26)
app//com.firebolt.jdbc.service.FireboltStatementService.executeStatementInternal(FireboltStatementService.java:119)
...
app//com.firebolt.jdbc.statement.FireboltStatement.executeUpdate(FireboltStatement.java:229)
```

- **WARN spam rate:** ~**189,830 WARNs/second** (`37966` matching WARNs in a 200ms window; ~95k total captured before logger was set to OFF). Message: `Could not read entire input stream for non query statement`.

### After fix

- `executeUpdate` returned `SQLException` (`FireboltException`) in **149 ms** with message:
  `Response stream was interrupted while draining a non-query statement; the statement may or may not have been applied`
- No hang; no WARN spam loop.

## Stage 3 — Live validation (env-gated, confirmatory)

**Env:** `FIREBOLT_CLIENT_ID`, `FIREBOLT_CLIENT_SECRET`, `FIREBOLT_ACCOUNT`, `FIREBOLT_DATABASE`, `FIREBOLT_ENGINE` (optional `FIREBOLT_ENVIRONMENT`, `FIREBOLT_REPRO_IDLE_MS`, `FIREBOLT_REPRO_BATCHES`). Skips when unset. Only touches `jdbc_repro_drop_me`.

**Observed (smoke, `FIREBOLT_REPRO_IDLE_MS=0`, 3 batches):**

- Ran against an internal test account (details omitted); 2 INSERT batches + cleanup succeeded. Idle-reset scenario not exercised.
- `CREATE TABLE` / 3× `INSERT` / `DROP TABLE` all succeeded in ~4s against the fixed driver.
- Multi-minute idle-out RST (6–10 minutes between batches) **not** exercised in this run; Stages 1–2 already prove the hang/fix. Cap remains 45 minutes via `@Timeout` if `FIREBOLT_REPRO_IDLE_MS` is set.

## Stage 4 — Fix and re-verify

### Behavioral change

| | Before | After |
|--|--------|--------|
| `InputStreamUtil.readAllBytes` | `while (true)` catch IOException → log WARN forever | drains with buffer; **`throws IOException`** on failure |
| `FireboltStatementService.executeStatementInternal` | calls `readAllBytes` then close | catch IOException → `FireboltException` with ambiguous-outcome message; always close in `finally` |
| Unit / MockWebServer | hang + WARN spam | fail fast with `SQLException` / `IOException` |

### Diff (core)

`InputStreamUtil.readAllBytes` now:

```java
public void readAllBytes(@Nullable InputStream is) throws IOException {
    if (is != null) {
        byte[] buffer = new byte[BUFFER_SIZE];
        while (is.read(buffer) != -1) {
            // discard
        }
    }
}
```

`executeStatementInternal` non-query branch:

```java
try {
    InputStreamUtil.readAllBytes(is);
} catch (IOException e) {
    throw new FireboltException(
            "Response stream was interrupted while draining a non-query statement; "
                    + "the statement may or may not have been applied",
            e);
} finally {
    CloseableUtil.close(is);
}
```

### Test suite

- `./gradlew test` — **BUILD SUCCESSFUL** (no regressions observed; PR #216 drain-to-completion still works on healthy streams via the buffered read-until-EOF loop).
- New tests:
  - `InputStreamUtilTest.shouldPropagateIoExceptionInsteadOfLooping`
  - `InputStreamUtilInfiniteLoopReproTest` (fixed assert + optional hang capture)
  - `Http2StreamResetExecuteUpdateHangTest` (fixed assert + optional hang/WARN capture)
  - `InputStreamDrainLiveReproTest` (env-gated Stage 3)

## How to re-run hang captures (pre-fix tree only)

```bash
export REPRO_INPUTSTREAM_HANG=true
./gradlew test --tests com.firebolt.jdbc.util.InputStreamUtilInfiniteLoopReproTest.captureHangStackWhenReproFlagSet

export REPRO_HTTP2_HANG=true
./gradlew test --tests com.firebolt.jdbc.service.Http2StreamResetExecuteUpdateHangTest.captureHangAndWarnSpamWhenReproFlagSet
```

On this fixed branch those captures correctly report that the hang is gone.

## Review follow-ups: SQLState 08007 + pool eviction

Engineering asked for two additions on top of the wrap-and-rethrow fix (no behavioral change to the drain itself):

1. **SQLState `08007` (transaction resolution unknown)** on the in-doubt drain failure. `FireboltStatementService.executeStatementInternal` now throws `new FireboltException(message, cause, SQLState.TRANSACTION_RESOLUTION_UNKNOWN)` so callers can machine-detect ambiguous statement outcome via `SQLException.getSQLState()`.
2. **Connection pool eviction** on drain failure: `StatementClient.evictConnectionPool()` → `OkHttpClient.connectionPool().evictAll()` via `StatementClientImpl`, invoked before the exception is thrown. Prevents the next statement from reusing a stale HTTP/2 connection after `RST_STREAM` / similar.

**Recovery test** (`Http2StreamResetExecuteUpdateHangTest.secondStatementSucceedsOnFreshConnectionAfterDrainFailure`): first INSERT gets partial body + CANCEL → `SQLException` with `SQLState=08007` and `evictConnectionPool()` verified; second INSERT on the same `FireboltStatement` succeeds. OkHttp `EventListener.connectionAcquired` shows two distinct `Connection` instances (second request does not reuse the reset connection). Unit coverage also in `FireboltStatementServiceTest.shouldSurfaceSqlState08007AndEvictPoolWhenNonQueryDrainFails`.
