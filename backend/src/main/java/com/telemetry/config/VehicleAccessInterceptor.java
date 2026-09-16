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
 * <p><b>왜 컨트롤러가 아니라 인터셉터인가</b>: 차량 하위 엔드포인트가 네 컨트롤러에 흩어져 있고
 * 앞으로도 는다. 컨트롤러마다 검사를 넣으면 <b>새 엔드포인트에서 조용히 빠진다</b> —
 * 실제로 WebSocket에만 검사가 있고 REST 전부가 빠져 있었다.
 *
 * <p><b>왜 403이 아니라 404인가</b>: 403은 "그 차량은 존재한다"를 알려준다. 남의 차량 ID를
 * 확인해 주는 셈이라, 없는 차량과 같은 응답을 준다.
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
