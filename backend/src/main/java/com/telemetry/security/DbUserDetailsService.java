package com.telemetry.security;

import com.telemetry.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * users 테이블 기반 인증(ADR-027). 2026-09-27까지는 {@code InMemoryUserDetailsManager}의 admin 한 명이었다.
 *
 * <p>비활성이거나 비밀번호 해시가 없는 계정은 <b>없는 계정과 같은 예외</b>를 던진다 — 로그인 실패 응답에서
 * "계정은 있는데 비활성"을 구분해 주면 사용자 이름 탐색에 쓰인다.
 */
@Service
@RequiredArgsConstructor
public class DbUserDetailsService implements UserDetailsService {

    private final UserRepository userRepository;

    @Override
    @Transactional(readOnly = true)
    public UserDetails loadUserByUsername(String username) {
        return userRepository.findByUsername(username)
            .filter(com.telemetry.entity.User::canLogin)
            .map(user -> org.springframework.security.core.userdetails.User.builder()
                .username(user.getUsername())
                .password(user.getPasswordHash())
                .authorities(List.of(new SimpleGrantedAuthority(user.getRole().authority())))
                .build())
            .orElseThrow(() -> new UsernameNotFoundException("사용자를 찾을 수 없습니다"));
    }
}
