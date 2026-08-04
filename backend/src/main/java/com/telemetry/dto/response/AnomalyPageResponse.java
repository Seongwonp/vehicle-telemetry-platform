package com.telemetry.dto.response;

import org.springframework.data.domain.Page;

import java.util.List;

public record AnomalyPageResponse(
    List<AnomalyResponse> content,
    long totalElements,
    int page,
    int size,
    boolean hasNext
) {
    public static AnomalyPageResponse from(Page<AnomalyResponse> result) {
        return new AnomalyPageResponse(
            result.getContent(), result.getTotalElements(), result.getNumber(),
            result.getSize(), result.hasNext());
    }
}
