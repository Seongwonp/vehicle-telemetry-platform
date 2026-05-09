package com.telemetry.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerConfig {

    @Bean
    public OpenAPI openAPI() {
        return new OpenAPI()
            .info(new Info()
                .title("Vehicle Telemetry Platform API")
                .version("v1.0")
                .description("""
                    차량 텔레메트리 데이터 수집 & 모니터링 플랫폼 REST API

                    **사용 방법:**
                    1. `/api/auth/login` 으로 JWT 토큰 발급
                    2. 우측 상단 **Authorize** 버튼 클릭
                    3. `Bearer <토큰>` 입력 후 API 호출
                    """)
            )
            .components(new Components()
                .addSecuritySchemes("bearerAuth",
                    new SecurityScheme()
                        .type(SecurityScheme.Type.HTTP)
                        .scheme("bearer")
                        .bearerFormat("JWT")
                        .description("JWT 액세스 토큰을 입력하세요")
                )
            );
    }
}
