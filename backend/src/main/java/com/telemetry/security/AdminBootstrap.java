package com.telemetry.security;

import com.telemetry.entity.Role;
import com.telemetry.entity.User;
import com.telemetry.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 관리자 계정을 {@code ADMIN_USERNAME}/{@code ADMIN_PASSWORD}와 맞춘다(ADR-027).
 *
 * <p>V4 마이그레이션이 관리자 행을 만들지만 BCrypt 해시는 SQL로 만들 수 없어 여기서 채운다.
 * <b>env가 관리자 비밀번호의 단일 기준</b>이다 — 해시가 env와 다르면 env 쪽으로 덮어쓴다. 그래야
 * InMemory 시절과 같은 운영 절차(.env 수정 후 재기동)가 유지된다. 일반 사용자 비밀번호는 건드리지 않는다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AdminBootstrap implements ApplicationRunner {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;

    @Value("${admin.username}")
    private String adminUsername;

    @Value("${admin.password}")
    private String adminPassword;

    @Override
    @Transactional
    public void run(ApplicationArguments args) {
        User admin = userRepository.findByUsername(adminUsername)
            .orElseGet(() -> new User(adminUsername, null, Role.ADMIN));

        boolean changed = false;
        if (admin.getRole() != Role.ADMIN || !admin.isActive()) {
            admin.setRole(Role.ADMIN);
            admin.setActive(true);
            changed = true;
        }
        if (admin.getPasswordHash() == null || !passwordEncoder.matches(adminPassword, admin.getPasswordHash())) {
            admin.setPasswordHash(passwordEncoder.encode(adminPassword));
            changed = true;
        }
        if (changed) {
            userRepository.save(admin);
            log.info("[AdminBootstrap] 관리자 계정을 env 기준으로 맞췄다 — username={}", adminUsername);
        }
    }
}
