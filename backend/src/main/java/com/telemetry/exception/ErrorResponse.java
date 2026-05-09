package com.telemetry.exception;

import lombok.Getter;

import java.time.Instant;

@Getter
public class ErrorResponse {

    private final String code;
    private final String message;
    private final String timestamp;

    public ErrorResponse(String code, String message) {
        this.code = code;
        this.message = message;
        this.timestamp = Instant.now().toString();
    }
}
