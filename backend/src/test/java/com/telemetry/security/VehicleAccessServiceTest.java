package com.telemetry.security;

import com.telemetry.repository.VehicleRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class VehicleAccessServiceTest {

    @Mock VehicleRepository vehicleRepository;
    @InjectMocks VehicleAccessService service;

    @Test
    void nonAdminCanAccessOnlyOwnedActiveVehicle() {
        var auth = new UsernamePasswordAuthenticationToken("user1", null, List.of());
        given(vehicleRepository.existsByVehicleIdAndOwnerAndActiveTrue("KR-GA-1234", "user1"))
            .willReturn(true);

        assertThat(service.canAccess(auth, "KR-GA-1234")).isTrue();
    }

    @Test
    void adminStillRequiresRegisteredActiveVehicle() {
        var auth = new UsernamePasswordAuthenticationToken("admin", null,
            List.of(new SimpleGrantedAuthority("ROLE_ADMIN")));
        given(vehicleRepository.existsByVehicleIdAndActiveTrue("KR-GA-1234")).willReturn(false);

        assertThat(service.canAccess(auth, "KR-GA-1234")).isFalse();
    }
}
