package com.firebolt.jdbc.client.account;

import org.json.JSONObject;

import java.util.Objects;

public class FireboltAccount {
    private final String id;
    private final String region;
    private final int infraVersion;

    public FireboltAccount(String id, String region, int infraVersion) {
        this.id = id;
        this.region = region;
        this.infraVersion = infraVersion;
    }

    @SuppressWarnings("unused") // used  by FireboltAccountRetriever that in turn calls its base class` method FireboltClient.jsonToObject() that calls this constructor by reflection
    FireboltAccount(JSONObject json) {
        this(json.getString("id"), json.getString("region"), json.optInt("infraVersion", 1));
    }

    public String getId() {
        return id;
    }

    public String getRegion() {
        return region;
    }

    public int getInfraVersion() {
        return infraVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FireboltAccount account = (FireboltAccount) o;
        return Objects.equals(id, account.id) && Objects.equals(region, account.region) && Objects.equals(infraVersion, account.infraVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, region, infraVersion);
    }
}
