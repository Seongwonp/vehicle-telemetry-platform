package com.telemetry.domain;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * 거부 경로의 추적 키. 원본 payload는 로그에 남기지 않는다(값이 섞인다) — 대신 SHA-256을 남겨
 * DLQ 레코드의 value와 대조할 수 있게 한다. MQTT 거부와 Kafka 저장 DLQ가 같은 함수를 써야
 * 두 로그의 {@code payloadSha256}이 같은 원본에서 같은 값이다(이벤트 상관관계 1단계, ADR-028).
 */
public final class PayloadDigest {

    private PayloadDigest() {}

    public static String sha256(String payload) {
        if (payload == null) return "null";
        try {
            return HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
