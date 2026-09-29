package com.telemetry.dto.request;

import com.telemetry.security.ValidPassword;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@Schema(description = "비밀번호 변경 요청 (본인)")
public class PasswordChangeRequest {

    // 현재 비밀번호에는 길이 규칙을 걸지 않는다 — 규칙이 바뀌기 전에 만든 비밀번호로도 변경할 수 있어야 한다.
    @NotBlank
    @Schema(description = "현재 비밀번호", example = "dummy-old-pw-not-real")
    private String currentPassword;

    @ValidPassword
    @Schema(description = "새 비밀번호 (8~72자)", example = "dummy-new-pw-not-real")
    private String newPassword;
}
