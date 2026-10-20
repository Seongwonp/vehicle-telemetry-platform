package com.telemetry.config;

import com.telemetry.exception.ResourceNotFoundException;
import com.telemetry.security.VehicleAccessService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.HandlerMapping;

import java.util.Map;

/**
 * `/api/vehicles/{vehicleId}/**` 요청이 그 차량에 접근할 수 있는 사용자인지 본다.
 *
 * <p>컨트롤러마다 넣으면 새 엔드포인트에서 검사가 빠지므로 인터셉터 한 곳에서 건다(ADR-025).
 * 403 대신 404를 준다. 403은 남의 차량 ID가 존재함을 알려준다.
 */
@Component
@RequiredArgsConstructor
public class VehicleAccessInterceptor implements HandlerInterceptor {

    private final VehicleAccessService vehicleAccessService;

    @Override
    @SuppressWarnings("unchecked")
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        Object attribute = request.getAttribute(HandlerMapping.URI_TEMPLATE_VARIABLES_ATTRIBUTE);
        String vehicleId = attribute instanceof Map<?, ?> variables
            ? ((Map<String, String>) variables).get("vehicleId")
            : null;
        // 목록·등록처럼 차량을 지정하지 않는 요청은 이 검사의 대상이 아니다.
        if (vehicleId == null) {
            return true;
        }

        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (vehicleAccessService.canAccess(authentication, vehicleId)) {
            return true;
        }
        throw new ResourceNotFoundException("등록되지 않은 차량입니다: " + vehicleId);
    }
}
