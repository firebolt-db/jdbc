/*
 * Copyright 2023 Firebolt Analytics, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * NOTICE: THIS FILE HAS BEEN MODIFIED BY Firebolt Analytics, Inc. UNDER COMPLIANCE WITH THE APACHE 2.0 LICENCE FROM THE ORIGINAL WORK
OF Square
 */

/*
 * Copyright (C) 2019 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.firebolt.jdbc.client.config;

import lombok.CustomLog;
import okhttp3.internal.concurrent.TaskRunner;
import okhttp3.internal.http2.Http2;
import java.io.Closeable;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

@CustomLog
public final class OkHttpDebugLogging {
    // Keep references to loggers to prevent their configuration from being GC'd.
    private static final CopyOnWriteArraySet<Logger> configuredLoggers = new CopyOnWriteArraySet<>();

    public static void enableHttp2() {
        enable(Http2.class.getName());
    }

    public static void enableTaskRunner() {
        enable(TaskRunner.class.getName());
    }

    public static Handler logHandler() {
        Logger logger = Logger.getLogger(OkHttpDebugLogging.class.getName());
        logger.setUseParentHandlers(false);
        Handler handler = new ConsoleHandler() {
            @Override
            public void publish(LogRecord record) {
                log.info(record.getMessage());
            }
        };

        logger.addHandler(handler);
        return handler;
    }

    public static Closeable enable(String loggerClass, Handler handler) {
        Logger logger = Logger.getLogger(loggerClass);
        if (configuredLoggers.add(logger)) {
            logger.addHandler(handler);
            logger.setLevel(Level.FINEST);
        }
        return () -> logger.removeHandler(handler);
    }

    public static Closeable enable(String name) {
        return enable(name, logHandler());
    }
}
