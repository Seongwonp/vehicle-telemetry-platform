package com.telemetry.service;

import com.telemetry.dto.request.UserCreateRequest;
import com.telemetry.dto.response.UserResponse;
import com.telemetry.entity.Role;
import com.telemetry.entity.User;
import com.telemetry.exception.ResourceConflictException;
import com.telemetry.exception.CurrentPasswordMismatchException;
import com.telemetry.exception.ResourceNotFoundException;
import com.telemetry.repository.UserRepository;
import com.telemetry.security.RefreshTokenService;
import org.springframework.security.authentication.BadCredentialsException;
import java.util.Optional;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.password.PasswordEncoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@DisplayName("UserService")
class UserServiceTest {

    @Mock UserRepository userRepository;
    @Mock PasswordEncoder passwordEncoder;
    @Mock RefreshTokenService refreshTokenService;
    @InjectMocks UserService userService;

    @Test
    @DisplayName("생성 시 비밀번호는 해시로 저장되고 응답에 해시가 없다")
    void create_해시저장() {
        UserCreateRequest request = new UserCreateRequest();
        request.setUsername("hong");
        request.setPassword("plain-password");
        request.setRole(Role.USER);
        given(userRepository.existsByUsername("hong")).willReturn(false);
        given(passwordEncoder.encode("plain-password")).willReturn("$2a$hashed");
        given(userRepository.save(any(User.class))).willAnswer(inv -> inv.getArgument(0));

        UserResponse response = userService.create(request);

        assertThat(response.getUsername()).isEqualTo("hong");
        assertThat(response.isLoginEnabled()).isTrue();
        verify(userRepository).save(any(User.class));
    }

    @Test
    @DisplayName("중복 username은 409")
    void create_중복_409() {
        UserCreateRequest request = new UserCreateRequest();
        request.setUsername("hong");
        request.setPassword("plain-password");
        request.setRole(Role.USER);
        given(userRepository.existsByUsername("hong")).willReturn(true);

        assertThatThrownBy(() -> userService.create(request)).isInstanceOf(ResourceConflictException.class);
        verify(userRepository, never()).save(any());
    }

    private static User account(boolean active) {
        User u = new User("hong", "$2a$old", Role.USER);
        u.setActive(active);
        return u;
    }

    @Test
    @DisplayName("변경 성공 — 새 해시 저장 + 그 사용자의 refresh 전부 폐기")
    void changePassword_성공() {
        User user = account(true);
        given(userRepository.findByUsername("hong")).willReturn(Optional.of(user));
        given(passwordEncoder.matches("old-pw", "$2a$old")).willReturn(true);
        given(passwordEncoder.matches("new-password-1", "$2a$old")).willReturn(false);
        given(passwordEncoder.encode("new-password-1")).willReturn("$2a$new");

        userService.changePassword("hong", "old-pw", "new-password-1");

        assertThat(user.getPasswordHash()).isEqualTo("$2a$new");
        verify(userRepository).save(user);
        verify(refreshTokenService).revokeAll("hong");
    }

    @Test
    @DisplayName("현재 비밀번호 불일치 — 해시도 refresh도 건드리지 않는다")
    void changePassword_현재비밀번호_틀림() {
        User user = account(true);
        given(userRepository.findByUsername("hong")).willReturn(Optional.of(user));
        given(passwordEncoder.matches("wrong", "$2a$old")).willReturn(false);

        assertThatThrownBy(() -> userService.changePassword("hong", "wrong", "new-password-1"))
            .isInstanceOf(CurrentPasswordMismatchException.class);
        assertThat(user.getPasswordHash()).isEqualTo("$2a$old");
        verify(userRepository, never()).save(any());
        verify(refreshTokenService, never()).revokeAll(any());
    }

    @Test
    @DisplayName("새 비밀번호가 현재와 같으면 거절")
    void changePassword_동일_거절() {
        given(userRepository.findByUsername("hong")).willReturn(Optional.of(account(true)));
        given(passwordEncoder.matches("same-password", "$2a$old")).willReturn(true);

        assertThatThrownBy(() -> userService.changePassword("hong", "same-password", "same-password"))
            .isInstanceOf(IllegalArgumentException.class);
        verify(refreshTokenService, never()).revokeAll(any());
    }

    @Test
    @DisplayName("비활성 계정은 없는 계정과 같이 거절 — 비밀번호 비교도 하지 않는다")
    void changePassword_비활성_거절() {
        given(userRepository.findByUsername("hong")).willReturn(Optional.of(account(false)));

        assertThatThrownBy(() -> userService.changePassword("hong", "old-pw", "new-password-1"))
            .isInstanceOf(BadCredentialsException.class);
        verify(passwordEncoder, never()).matches(any(), any());
        verify(refreshTokenService, never()).revokeAll(any());
    }

    @Test
    @DisplayName("관리자 초기화 — 새 해시 저장 + 대상의 refresh 폐기")
    void resetPassword_성공() {
        User user = account(true);
        given(userRepository.findByUsername("hong")).willReturn(Optional.of(user));
        given(passwordEncoder.encode("new-password-1")).willReturn("$2a$reset");

        userService.resetPassword("hong", "new-password-1");

        assertThat(user.getPasswordHash()).isEqualTo("$2a$reset");
        verify(refreshTokenService).revokeAll("hong");
    }

    @Test
    @DisplayName("관리자 초기화 — 없는 사용자는 404 계열, 비활성 사용자는 해시만 바꾸고 active는 그대로")
    void resetPassword_없는사용자와_비활성() {
        given(userRepository.findByUsername("ghost")).willReturn(Optional.empty());
        assertThatThrownBy(() -> userService.resetPassword("ghost", "new-password-1"))
            .isInstanceOf(ResourceNotFoundException.class);

        User off = account(false);
        given(userRepository.findByUsername("hong")).willReturn(Optional.of(off));
        given(passwordEncoder.encode("new-password-1")).willReturn("$2a$reset");
        userService.resetPassword("hong", "new-password-1");
        assertThat(off.getPasswordHash()).isEqualTo("$2a$reset");
        assertThat(off.isActive()).isFalse();
    }
}
