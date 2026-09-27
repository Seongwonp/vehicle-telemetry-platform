package com.telemetry.security;

import com.telemetry.entity.Role;
import com.telemetry.entity.User;
import com.telemetry.repository.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@DisplayName("AdminBootstrap — env가 관리자 비밀번호의 단일 기준")
class AdminBootstrapTest {

    private final UserRepository userRepository = mock(UserRepository.class);
    private final PasswordEncoder encoder = new BCryptPasswordEncoder(4);
    private AdminBootstrap bootstrap;

    @BeforeEach
    void setUp() {
        bootstrap = new AdminBootstrap(userRepository, encoder);
        ReflectionTestUtils.setField(bootstrap, "adminUsername", "admin");
        ReflectionTestUtils.setField(bootstrap, "adminPassword", "env-secret");
    }

    @Test
    @DisplayName("V4가 만든 해시 없는 관리자 행에 env 비밀번호 해시를 채운다")
    void 해시없으면_채운다() {
        given(userRepository.findByUsername("admin")).willReturn(Optional.of(new User("admin", null, Role.ADMIN)));

        bootstrap.run(null);

        ArgumentCaptor<User> saved = ArgumentCaptor.forClass(User.class);
        verify(userRepository).save(saved.capture());
        assertThat(encoder.matches("env-secret", saved.getValue().getPasswordHash())).isTrue();
    }

    @Test
    @DisplayName("행이 없으면 만든다 — Flyway placeholder와 env의 username이 다를 때의 안전망")
    void 없으면_만든다() {
        given(userRepository.findByUsername("admin")).willReturn(Optional.empty());

        bootstrap.run(null);

        ArgumentCaptor<User> saved = ArgumentCaptor.forClass(User.class);
        verify(userRepository).save(saved.capture());
        assertThat(saved.getValue().getRole()).isEqualTo(Role.ADMIN);
        assertThat(saved.getValue().isActive()).isTrue();
    }

    @Test
    @DisplayName("해시가 env와 이미 맞으면 아무것도 쓰지 않는다 — 기동마다 BCrypt를 다시 돌리지 않는다")
    void 이미맞으면_저장없음() {
        given(userRepository.findByUsername("admin"))
            .willReturn(Optional.of(new User("admin", encoder.encode("env-secret"), Role.ADMIN)));

        bootstrap.run(null);

        verify(userRepository, never()).save(any());
    }

    @Test
    @DisplayName("env 비밀번호가 바뀌면 해시를 덮어쓴다")
    void 바뀌면_덮어쓴다() {
        given(userRepository.findByUsername("admin"))
            .willReturn(Optional.of(new User("admin", encoder.encode("old-secret"), Role.ADMIN)));

        bootstrap.run(null);

        ArgumentCaptor<User> saved = ArgumentCaptor.forClass(User.class);
        verify(userRepository).save(saved.capture());
        assertThat(encoder.matches("env-secret", saved.getValue().getPasswordHash())).isTrue();
    }
}
