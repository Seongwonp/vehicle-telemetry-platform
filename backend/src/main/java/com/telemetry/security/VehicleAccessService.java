package com.telemetry.security;

import com.telemetry.repository.VehicleRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class VehicleAccessService {

    private final VehicleRepository vehicleRepository;

    public boolean canAccess(Authentication authentication, String vehicleId) {
        if (authentication == null || !authentication.isAuthenticated()) {
            return false;
        }
        boolean admin = authentication.getAuthorities().stream()
            .anyMatch(authority -> authority.getAuthority().equals("ROLE_ADMIN"));
        if (admin) {
            return vehicleRepository.existsByVehicleIdAndActiveTrue(vehicleId);
        }
        return vehicleRepository.existsByVehicleIdAndOwnerAndActiveTrue(
            vehicleId, authentication.getName());
    }
}
