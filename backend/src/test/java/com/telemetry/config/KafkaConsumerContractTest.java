package com.telemetry.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.ConsumerFactory;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 컨슈머 그룹 멤버십 계약 회귀 테스트.
 *
 * <p><b>왜 필요한가</b>: 여기 걸린 값들은 정상 동작 중에는 아무 차이도 만들지 않는다.
 * 인스턴스가 죽거나 내려갈 때만 드러나서, 잘못 바뀌어도 일반 테스트로는 안 잡힌다.
 *
 * <p>실제로 {@code session.timeout.ms}는 오랫동안 <b>명시돼 있지 않았다.</b>
 * 2026-09-07에 저장 인스턴스를 내려보니 컨슈머 그룹 재할당까지 44초가 걸렸는데,
 * 그 44초는 우리가 정한 값이 아니라 Kafka 클라이언트 기본값(45초)이었다. 측정해서
 * Runbook에 적은 동작이 문서에 없는 기본값에 매달려 있던 셈이다 —
 * {@code docs/verification/2026-09-07-compose-scale-profile.md}.
 *
 * <p>이 테스트는 두 가지를 따로 지킨다.
 * <ol>
 *   <li><b>실제 {@code application.yml}에 값이 있는가</b> — 지우면 조용히 기본값으로
 *       돌아가는데, 지금은 기본값과 같아서 동작으로는 안 드러난다. 나중에 클라이언트가
 *       기본값을 바꾸면 그때야 드러난다.</li>
 *   <li><b>그 값이 실제로 컨슈머에 도달하는가</b> — {@code spring.kafka.listener.*}가
 *       커스텀 팩토리에 안 물려져서 <b>무효 설정</b>이었던 전례가 있다
 *       ({@link KafkaListenerContractTest}).</li>
 * </ol>
 */
@DisplayName("Kafka 컨슈머 멤버십 계약")
class KafkaConsumerContractTest {

    /**
     * 실제 {@code application.yml}을 그대로 읽어 넣는다. 테스트에 값을 다시 적으면
     * "테스트가 통과하는데 운영 설정에는 없는" 상태를 못 잡는다 — 이 테스트의 목적이
     * 정확히 그것이다.
     *
     * <p>바인딩은 지연 평가라 {@code spring.kafka.*} 밖의 플레이스홀더
     * ({@code ${POSTGRES_USER}} 등)는 해석되지 않는다. Kafka 자동설정만 올린다.
     */
    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
            .withInitializer(context -> {
                try {
                    List<PropertySource<?>> sources = new YamlPropertySourceLoader()
                        .load("application.yml", new ClassPathResource("application.yml"));
                    sources.forEach(context.getEnvironment().getPropertySources()::addLast);
                } catch (IOException e) {
                    throw new IllegalStateException("application.yml을 읽지 못했다", e);
                }
            })
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class));
    }

    @Test
    @DisplayName("session.timeout.ms가 application.yml에 명시돼 있고 컨슈머에 도달한다")
    void 세션_타임아웃이_명시돼_있다() {
        runner().run(context -> {
            ConsumerFactory<?, ?> consumerFactory = context.getBean(ConsumerFactory.class);

            // 45,000ms는 마침 클라이언트 기본값과 같다. **같아서 지워도 티가 안 난다**는
            // 것이 이 단언의 존재 이유다. 값을 바꾸려면 스케일 다운 지연(측정 44초)과
            // 리밸런싱 민감도의 트레이드오프를 먼저 재라 — docs/runbook/storage-scale-out.md.
            assertThat(consumerFactory.getConfigurationProperties())
                .containsEntry(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "45000");
        });
    }

    @Test
    @DisplayName("정적 멤버십이 켜져 있다 — 이 값이 없으면 위 타임아웃은 의미가 달라진다")
    void 정적_멤버십이_켜져_있다() {
        runner().run(context -> {
            ConsumerFactory<?, ?> consumerFactory = context.getBean(ConsumerFactory.class);

            // group.instance.id가 없으면 컨슈머가 종료할 때 LeaveGroup을 보내고 즉시
            // 재할당된다. 그러면 "내리는 데 45초"라는 서술 자체가 틀린 말이 된다.
            assertThat(consumerFactory.getConfigurationProperties())
                .containsEntry(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "telemetry-backend-consumer");
        });
    }

    @Test
    @DisplayName("환경변수로 조정할 수 있다 — 재빌드 없이 바꿀 수 있어야 한다")
    void 환경변수로_조정된다() {
        runner()
            .withPropertyValues(
                "KAFKA_SESSION_TIMEOUT_MS=20000",
                "GROUP_INSTANCE_ID_BASE=telemetry-storage-1")
            .run(context -> {
                ConsumerFactory<?, ?> consumerFactory = context.getBean(ConsumerFactory.class);

                assertThat(consumerFactory.getConfigurationProperties())
                    .containsEntry(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "20000")
                    .containsEntry(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "telemetry-storage-1");
            });
    }

    /** 위 runner에 {@link KafkaConfig}까지 올린다 — 정적 멤버십 끄기 커스터마이저가 거기 있다. */
    private ApplicationContextRunner runnerWithKafkaConfig() {
        return runner()
            .withUserConfiguration(KafkaConfig.class)
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new);
    }

    @Test
    @DisplayName("GROUP_INSTANCE_ID_BASE가 비면 정적 멤버십이 꺼진다 — 속성 자체가 사라진다")
    void 빈_값이면_정적_멤버십이_꺼진다() {
        // Kafka는 group.instance.id에 빈 문자열을 허용하지 않는다(NonEmptyString 검증).
        // 그래서 "빈 값 = 끄기"로 해석하고 속성을 제거한다 — 안 그러면 기동이 실패한다.
        runnerWithKafkaConfig()
            .withPropertyValues("GROUP_INSTANCE_ID_BASE=")
            .run(context -> {
                ConsumerFactory<?, ?> consumerFactory = context.getBean(ConsumerFactory.class);

                assertThat(consumerFactory.getConfigurationProperties())
                    .doesNotContainKey(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG);
            });
    }

    @Test
    @DisplayName("값이 있으면 그대로 남는다 — 기본은 켜짐이다")
    void 값이_있으면_정적_멤버십이_유지된다() {
        runnerWithKafkaConfig().run(context -> {
            ConsumerFactory<?, ?> consumerFactory = context.getBean(ConsumerFactory.class);

            assertThat(consumerFactory.getConfigurationProperties())
                .containsEntry(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "telemetry-backend-consumer");
        });
    }
}
