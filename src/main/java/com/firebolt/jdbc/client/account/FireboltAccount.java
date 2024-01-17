package com.firebolt.jdbc.client.account;

import org.json.JSONObject;

import java.util.Objects;

public class FireboltAccount {
    private final String id;
    private final String region;

    public FireboltAccount(String id, String region) {
        this.id = id;
        this.region = region;
    }

    FireboltAccount(JSONObject json) {
        this(json.getString("id"), json.getString("region"));
    }

    public String getId() {
        return id;
    }

    public String getRegion() {
        return region;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FireboltAccount account = (FireboltAccount) o;
        return Objects.equals(id, account.id) && Objects.equals(region, account.region);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, region);
    }
}
