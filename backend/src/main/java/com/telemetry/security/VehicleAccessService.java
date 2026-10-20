package com.telemetry.security;

import com.telemetry.repository.VehicleRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

/**
 * 차량 단위 접근 권한. 관리자는 활성 차량 전부, 그 외 사용자는 자기 소유 차량만.
 * WebSocket 구독과 REST({@link com.telemetry.config.VehicleAccessInterceptor})가 같은 규칙을 쓴다(ADR-025).
 */
@Service
@RequiredArgsConstructor
public class VehicleAccessService {

    private final VehicleRepository vehicleRepository;

    public boolean canAccess(Authentication authentication, String vehicleId) {
        if (authentication == null || !authentication.isAuthenticated()) {
            return false;
        }
        if (isAdmin(authentication)) {
            return vehicleRepository.existsByVehicleIdAndActiveTrue(vehicleId);
        }
        return vehicleRepository.existsByVehicleIdAndOwner_UsernameAndActiveTrue(
            vehicleId, authentication.getName());
    }

    /** 관리자는 소유자가 아니어도 접근하고, 등록할 때 다른 사람을 소유자로 지정할 수 있다. */
    public boolean isAdmin(Authentication authentication) {
        return authentication != null
            && authentication.isAuthenticated()
            && authentication.getAuthorities().stream()
                .anyMatch(authority -> authority.getAuthority().equals("ROLE_ADMIN"));
    }
}
