package com.telemetry.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@Schema(description = "리프레시 토큰 요청")
public class RefreshRequest {

    @NotBlank
    @Schema(description = "로그인 시 발급받은 리프레시 토큰")
    private String refreshToken;
}
