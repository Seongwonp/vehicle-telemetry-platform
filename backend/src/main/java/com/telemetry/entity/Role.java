package com.telemetry.entity;

public enum Role {
    ADMIN, USER;

    public String authority() {
        return "ROLE_" + name();
    }
}
