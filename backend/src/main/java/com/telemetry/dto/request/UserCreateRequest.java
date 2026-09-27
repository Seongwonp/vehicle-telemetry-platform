package com.telemetry.dto.request;

import com.telemetry.entity.Role;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.Data;

@Data
@Schema(description = "사용자 생성 요청 (관리자 전용)")
public class UserCreateRequest {

    @NotBlank
    @Pattern(regexp = "^[a-z0-9._-]{3,50}$", message = "username은 소문자/숫자/._- 3~50자")
    @Schema(description = "로그인 이름", example = "hong")
    private String username;

    @NotBlank
    @Size(min = 8, max = 72, message = "비밀번호는 8~72자")
    @Schema(description = "비밀번호", example = "correct-horse-battery")
    private String password;

    @NotNull
    @Schema(description = "역할", example = "USER")
    private Role role;
}
