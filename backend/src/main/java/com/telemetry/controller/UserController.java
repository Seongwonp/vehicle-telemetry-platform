package com.telemetry.controller;

import com.telemetry.dto.request.PasswordResetRequest;
import com.telemetry.dto.request.UserCreateRequest;
import com.telemetry.dto.response.UserResponse;
import com.telemetry.service.UserService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 사용자 관리 — 관리자 전용. 자가 가입은 두지 않는다(차량 ID 선점·소유권 주장 경로가 된다, ADR-027).
 */
@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
@Tag(name = "User", description = "사용자 관리 API (관리자 전용)")
@SecurityRequirement(name = "bearerAuth")
@PreAuthorize("hasRole('ADMIN')")
public class UserController {

    private final UserService userService;

    @PostMapping
    @Operation(summary = "사용자 생성", description = "관리자만 호출할 수 있다. 자가 가입 없음")
    public ResponseEntity<UserResponse> create(@Valid @RequestBody UserCreateRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(userService.create(request));
    }

    @PutMapping("/{username}/password")
    @Operation(summary = "비밀번호 초기화",
        description = "관리자가 대상 사용자의 비밀번호를 새 값으로 바꾸고 그 사용자의 refresh token을 모두 폐기한다. "
            + "이미 발급된 access token은 자체 만료까지 유효하다")
    public ResponseEntity<Void> resetPassword(
        @PathVariable String username, @Valid @RequestBody PasswordResetRequest request
    ) {
        userService.resetPassword(username, request.getNewPassword());
        return ResponseEntity.noContent().build();
    }

    @GetMapping
    @Operation(summary = "사용자 목록")
    public ResponseEntity<List<UserResponse>> findAll() {
        return ResponseEntity.ok(userService.findAll());
    }
}
