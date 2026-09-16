package com.telemetry.security;

import com.telemetry.repository.VehicleRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

/**
 * 차량 단위 접근 권한 — <b>관리자는 등록된 활성 차량 전부, 그 외 사용자는 자기 소유 차량만.</b>
 *
 * <p>2026-09-16까지 이 규칙은 <b>WebSocket 구독에만</b> 걸려 있었다. REST는 아무 검사도 하지 않아서
 * 인증만 하면 남의 차량 텔레메트리·이상 이력·진단을 읽을 수 있었다. 같은 규칙을 REST에도 건다
 * ({@link com.telemetry.config.VehicleAccessInterceptor}).
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
        return vehicleRepository.existsByVehicleIdAndOwnerAndActiveTrue(
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
