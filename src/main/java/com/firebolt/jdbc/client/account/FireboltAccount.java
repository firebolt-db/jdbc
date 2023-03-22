package com.firebolt.jdbc.client.account;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class FireboltAccount {
    @JsonProperty
    private final String id;
    @JsonProperty
    private final String region;

    @JsonCreator
    public FireboltAccount(@JsonProperty("id") String id, @JsonProperty("region") String region) {
        this.id = id;
        this.region = region;
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
