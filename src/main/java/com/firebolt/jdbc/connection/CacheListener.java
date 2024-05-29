package com.firebolt.jdbc.connection;

import java.util.EventListener;

public interface CacheListener extends EventListener {
    void cleanup();
}
