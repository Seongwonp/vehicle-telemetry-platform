package com.telemetry.service;

import com.telemetry.dto.request.UserCreateRequest;
import com.telemetry.dto.response.UserResponse;
import com.telemetry.entity.User;
import com.telemetry.exception.ResourceConflictException;
import com.telemetry.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

    @Transactional(readOnly = true)
    public List<UserResponse> findAll() {
        return userRepository.findAllByOrderByIdAsc().stream().map(UserResponse::new).toList();
    }
}
