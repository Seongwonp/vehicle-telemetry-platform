package com.telemetry.security;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * 토큰은 유효한데 계정이 없거나 비활성인 경우(DB 기반 사용자, ADR-027) 필터가 예외를 전파하지 않고
 * <b>인증 없이 체인을 계속</b>하는지 — 그래야 entry point가 401을 만들고 500으로 새지 않는다.
 */
@DisplayName("JwtAuthenticationFilter — 사라진·비활성 계정")
class JwtAuthenticationFilterTest {

    private final JwtTokenProvider provider = mock(JwtTokenProvider.class);
    private final UserDetailsService users = mock(UserDetailsService.class);
    private final FilterChain chain = mock(FilterChain.class);

    @AfterEach
    void clear() {
        SecurityContextHolder.clearContext();
    }

    private MockHttpServletRequest requestWithBearer() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader("Authorization", "Bearer token");
        return request;
    }

    @Test
    @DisplayName("계정을 못 찾으면 예외 없이 통과하고 인증은 비어 있다")
    void 계정없음_인증없이_통과() throws Exception {
        given(provider.validate("token")).willReturn(true);
        given(provider.getUsername("token")).willReturn("gone");
        given(users.loadUserByUsername("gone")).willThrow(new UsernameNotFoundException("x"));

        new JwtAuthenticationFilter(provider, users).doFilter(requestWithBearer(), new MockHttpServletResponse(), chain);

        verify(chain).doFilter(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
        assertThat(SecurityContextHolder.getContext().getAuthentication()).isNull();
    }

    @Test
    @DisplayName("정상 계정이면 인증이 세워진다")
    void 정상계정_인증() throws Exception {
        given(provider.validate("token")).willReturn(true);
        given(provider.getUsername("token")).willReturn("hong");
        given(users.loadUserByUsername("hong")).willReturn(User.withUsername("hong").password("x").roles("USER").build());

        new JwtAuthenticationFilter(provider, users).doFilter(requestWithBearer(), new MockHttpServletResponse(), chain);

        assertThat(SecurityContextHolder.getContext().getAuthentication().getName()).isEqualTo("hong");
    }
}
