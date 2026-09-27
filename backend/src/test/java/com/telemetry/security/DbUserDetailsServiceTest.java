package com.telemetry.security;

import com.telemetry.entity.Role;
import com.telemetry.entity.User;
import com.telemetry.repository.UserRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
@DisplayName("DB 기반 UserDetailsService")
class DbUserDetailsServiceTest {

    @Mock UserRepository userRepository;
    @InjectMocks DbUserDetailsService service;

    @Test
    @DisplayName("활성이고 해시가 있는 계정은 ROLE_ 접두 권한으로 로드된다")
    void 정상계정_로드() {
        given(userRepository.findByUsername("hong")).willReturn(Optional.of(new User("hong", "$2a$hash", Role.USER)));

        UserDetails details = service.loadUserByUsername("hong");

        assertThat(details.getPassword()).isEqualTo("$2a$hash");
        assertThat(details.getAuthorities()).extracting("authority").containsExactly("ROLE_USER");
    }

    @Test
    @DisplayName("비밀번호 해시가 없는 계정(V4 백필)은 없는 계정과 같은 예외다")
    void 해시없음_같은예외() {
        given(userRepository.findByUsername("legacy")).willReturn(Optional.of(new User("legacy", null, Role.USER)));

        assertThatThrownBy(() -> service.loadUserByUsername("legacy")).isInstanceOf(UsernameNotFoundException.class);
    }

    @Test
    @DisplayName("비활성 계정도 같은 예외다 — 존재 여부를 응답으로 구분해 주지 않는다")
    void 비활성_같은예외() {
        User user = new User("off", "$2a$hash", Role.USER);
        user.setActive(false);
        given(userRepository.findByUsername("off")).willReturn(Optional.of(user));

        assertThatThrownBy(() -> service.loadUserByUsername("off")).isInstanceOf(UsernameNotFoundException.class);
    }
}
