package com.telemetry.dto.request;

import com.telemetry.security.ValidPassword;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

@Data
@Schema(description = "비밀번호 초기화 요청 (관리자 전용)")
public class PasswordResetRequest {

    @ValidPassword
    @Schema(description = "새 비밀번호 (8~72자)", example = "dummy-new-pw-not-real")
    private String newPassword;
}
