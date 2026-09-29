package com.telemetry.service;

import com.telemetry.dto.request.UserCreateRequest;
import com.telemetry.dto.response.UserResponse;
import com.telemetry.entity.User;
import com.telemetry.exception.CurrentPasswordMismatchException;
import com.telemetry.exception.ResourceConflictException;
import com.telemetry.exception.ResourceNotFoundException;
import com.telemetry.security.RefreshTokenService;
import com.telemetry.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final RefreshTokenService refreshTokenService;

    @Transactional
    public UserResponse create(UserCreateRequest request) {
        if (userRepository.existsByUsername(request.getUsername())) {
            throw new ResourceConflictException("이미 존재하는 사용자입니다: " + request.getUsername());
        }
        User user = new User(request.getUsername(), passwordEncoder.encode(request.getPassword()), request.getRole());
        User saved = userRepository.save(user);
        log.info("사용자 생성 — username={} role={}", saved.getUsername(), saved.getRole());
        return new UserResponse(saved);
    }

    /**
     * 본인 비밀번호 변경. 로그인할 수 없는 계정(비활성·해시 없음)은 없는 계정과 같이 거절한다(로그인·refresh와 같은 규칙).
     * refresh 폐기가 같은 트랜잭션 안이라 Redis 오류가 나면 비밀번호 변경도 되돌아간다 — 비밀번호는 바뀌었는데
     * 옛 세션이 살아 있는 상태를 만들지 않는다.
     */
    @Transactional
    public void changePassword(String username, String currentPassword, String newPassword) {
        User user = userRepository.findByUsername(username)
            .filter(User::canLogin)
            .orElseThrow(() -> new BadCredentialsException("사용자를 찾을 수 없습니다"));
        if (!passwordEncoder.matches(currentPassword, user.getPasswordHash())) {
            throw new CurrentPasswordMismatchException();
        }
        if (passwordEncoder.matches(newPassword, user.getPasswordHash())) {
            throw new IllegalArgumentException("새 비밀번호가 현재 비밀번호와 같습니다");
        }
        user.setPasswordHash(passwordEncoder.encode(newPassword));
        userRepository.save(user);
        refreshTokenService.revokeAll(username);
        log.info("비밀번호 변경 — username={}", username);
    }

    /**
     * 관리자 초기화. 대상이 비활성이어도 해시는 바꾼다(비활성 백필 계정의 비밀번호를 미리 정해 둘 수 있다) —
     * {@code active}는 건드리지 않으므로 로그인 가능 여부는 그대로다.
     */
    @Transactional
    public void resetPassword(String username, String newPassword) {
        User user = userRepository.findByUsername(username)
            .orElseThrow(() -> new ResourceNotFoundException("사용자를 찾을 수 없습니다: " + username));
        user.setPasswordHash(passwordEncoder.encode(newPassword));
        userRepository.save(user);
        refreshTokenService.revokeAll(username);
        log.info("비밀번호 초기화(관리자) — username={}", username);
    }

    @Transactional(readOnly = true)
    public List<UserResponse> findAll() {
        return userRepository.findAllByOrderByIdAsc().stream().map(UserResponse::new).toList();
    }
}
