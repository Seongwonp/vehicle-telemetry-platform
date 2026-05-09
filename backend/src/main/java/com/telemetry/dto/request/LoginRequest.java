package com.telemetry.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@Schema(description = "로그인 요청")
public class LoginRequest {

    @NotBlank
    @Schema(description = "사용자 이름", example = "admin")
    private String username;

    @NotBlank
    @Schema(description = "비밀번호", example = "changeme")
    private String password;
}
