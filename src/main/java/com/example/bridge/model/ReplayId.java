package com.example.bridge.model;

import java.util.Arrays;
import java.util.Objects;

/**
 * Value object wrapping an opaque Salesforce replay ID (byte array).
 */
public final class ReplayId {

    private final byte[] value;

    public ReplayId(byte[] value) {
        Objects.requireNonNull(value, "Replay ID value must not be null");
        this.value = Arrays.copyOf(value, value.length);
    }

    public byte[] getValue() {
        return Arrays.copyOf(value, value.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplayId replayId = (ReplayId) o;
        return Arrays.equals(value, replayId.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    public String toString() {
        return "ReplayId{length=" + value.length + "}";
    }
}
