package com.telemetry.dto.response;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
@Schema(description = "로그인 응답 (JWT 토큰)")
public class LoginResponse {

    @Schema(description = "JWT 액세스 토큰")
    private final String accessToken;

    @Schema(description = "토큰 타입", example = "Bearer")
    private final String tokenType = "Bearer";

    @Schema(description = "만료 시간 (ms)", example = "86400000")
    private final long expiresIn;
}
