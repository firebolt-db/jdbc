package com.firebolt.jdbc.testutils;

import java.util.Set;

public class TestTag {

    // tests that are run against firebolt 1.0
    public static final String V1 = "v1";

    // tests that are run against firebolt 2.0
    public static final String V2 = "v2";

    // tests that are run against firebolt core
    public static final String CORE = "core";

    // tests that are slow
    public static final String SLOW = "slow";

    public static Set<String> getAllSupportedTags() {
        return Set.of(V1, V2, CORE, SLOW);
    }
}

