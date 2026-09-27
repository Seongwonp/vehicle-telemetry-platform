package com.telemetry.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.kafka.core.ProducerFactory;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 프로듀서 순서 보장 계약 회귀 테스트(ADR-026).
 *
 * <p>"정상 경로 역전 0"({@code load-test/order-integrity})의 근거인
 * {@code enable.idempotence=true} + {@code max.in.flight.requests.per.connection ≤ 5}는
 * 실험 당시 클라이언트 기본값이었고 {@code application.yml}에는 없었다. 기본값에 기대면
 * 클라이언트 업그레이드나 {@code retries}/{@code acks} 변경으로 조용히 꺼질 수 있다 —
 * {@link KafkaConsumerContractTest}의 {@code session.timeout.ms}와 같은 종류의 문제다.
 *
 * <p>{@link KafkaConsumerContractTest}처럼 실제 {@code application.yml}을 읽는다.
 */
@DisplayName("Kafka 프로듀서 순서 보장 계약")
class KafkaProducerContractTest {

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
    @DisplayName("idempotence와 in-flight 상한이 application.yml에 명시돼 있고 프로듀서에 도달한다")
    void 순서_보장_설정이_명시돼_있다() {
        runner().run(context -> {
            ProducerFactory<?, ?> producerFactory = context.getBean(ProducerFactory.class);

            // 둘 다 Kafka 3.0+ 기본값과 같다. **같아서 지워도 티가 안 난다**는 것이 이 단언의
            // 존재 이유다. in.flight를 5 넘게 올리면 idempotence가 순서를 보장하지 못한다.
            assertThat(producerFactory.getConfigurationProperties())
                .containsEntry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
                .containsEntry(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        });
    }

    @Test
    @DisplayName("acks=all이다 — idempotence는 acks=all이 아니면 기동에 실패한다")
    void acks가_all이다() {
        runner().run(context -> {
            ProducerFactory<?, ?> producerFactory = context.getBean(ProducerFactory.class);

            assertThat(producerFactory.getConfigurationProperties())
                .containsEntry(ProducerConfig.ACKS_CONFIG, "all");
        });
    }
}
