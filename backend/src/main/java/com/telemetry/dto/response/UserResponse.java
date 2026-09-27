package com.telemetry.dto.response;

import com.telemetry.entity.Role;
import com.telemetry.entity.User;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;

import java.time.Instant;

@Getter
@Schema(description = "사용자 응답 — 비밀번호 해시는 나가지 않는다")
public class UserResponse {
    private final Long id;
    private final String username;
    private final Role role;
    private final boolean active;
    @Schema(description = "로그인 가능 여부 (활성이고 비밀번호가 설정됨)")
    private final boolean loginEnabled;
    private final Instant createdAt;

    public UserResponse(User user) {
        this.id = user.getId();
        this.username = user.getUsername();
        this.role = user.getRole();
        this.active = user.isActive();
        this.loginEnabled = user.canLogin();
        this.createdAt = user.getCreatedAt();
    }
}
