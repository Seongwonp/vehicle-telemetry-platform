package com.telemetry.service;

import com.telemetry.dto.request.UserCreateRequest;
import com.telemetry.dto.response.UserResponse;
import com.telemetry.entity.Role;
import com.telemetry.entity.User;
import com.telemetry.exception.ResourceConflictException;
import com.telemetry.repository.UserRepository;
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
}
