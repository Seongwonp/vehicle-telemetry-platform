package com.telemetry.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ClassPathResource;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * health 그룹 구성이 <b>Redis 장애 정책과 모순되지 않는지</b> 고정한다.
 *
 * <p>정책은 "일반 조회는 Redis 없이도 fail-open으로 처리한다"이다. 그런데 readiness에
 * redis가 들어가면, 프록시·오케스트레이터가 readiness를 보는 순간 <b>모든 인스턴스가
 * 트래픽에서 빠져</b> 그 요청이 아예 도달하지 못한다 — 살리려고 만든 경로를 라우팅이 닫는다.
 * 지금 저장소에는 readiness를 보는 소비자가 없어서 이 모순이 <b>실측으로는 안 드러난다.</b>
 * 그래서 설정 자체를 테스트로 묶는다. ({@code docs/redis-failure-policy.md} §9-1)
 */
@DisplayName("health 그룹 — Redis 장애 정책과의 정합성")
class HealthGroupPolicyTest {

    private static Properties appYaml() {
        YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
        yaml.setResources(new ClassPathResource("application.yml"));
        return yaml.getObject();
    }

    private static List<String> include(Properties p, String group) {
        String v = p.getProperty("management.endpoint.health.group." + group + ".include", "");
        return Arrays.stream(v.split(",")).map(String::trim).filter(s -> !s.isEmpty()).toList();
    }

    @Test
    @DisplayName("readiness에 redis가 없다 — 있으면 fail-open 경로를 라우팅이 닫는다")
    void readiness에_redis_없음() {
        assertThat(include(appYaml(), "readiness"))
            .as("Redis는 전 인스턴스가 공유한다. readiness에 넣으면 Redis 장애 때 모든 인스턴스가 "
                + "빠져 일반 조회 fail-open이 무의미해진다")
            .isNotEmpty()
            .doesNotContain("redis");
    }

    @Test
    @DisplayName("liveness는 외부 의존을 보지 않는다 — 재시작해도 외부 저장소는 안 살아난다")
    void liveness는_외부의존_없음() {
        assertThat(include(appYaml(), "liveness"))
            .containsExactly("livenessState");
    }

    @Test
    @DisplayName("Redis 상태는 종합 health에서 여전히 보인다 — 숨긴 게 아니라 옮긴 것이다")
    void 종합_health에서_redis는_보인다() {
        assertThat(appYaml().getProperty("management.health.redis.enabled", "true"))
            .as("redis indicator를 끄면 운영자가 Redis 장애를 health로 볼 곳이 사라진다")
            .isNotEqualTo("false");
    }
}
