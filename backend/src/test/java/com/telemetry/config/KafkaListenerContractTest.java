package com.telemetry.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 리스너 컨테이너 계약 회귀 테스트.
 *
 * <p><b>왜 필요한가</b>: 이 값들은 설정 파일과 Java 양쪽에 나타난다. 예전에는 커스텀
 * 팩토리가 {@code application.yml}의 {@code spring.kafka.listener.*}를 물려받지 않고
 * 같은 값을 Java에 하드코딩하고 있었다 — **yml을 고쳐도 동작이 안 바뀌는데 읽는 사람은
 * 바뀔 거라고 믿는** 상태였다. 우연히 값이 같아서 아무도 몰랐다.
 *
 * <p>동작이 아니라 배선(wiring)이라 일반 테스트로는 안 잡힌다. 그래서 설정값을
 * **기본값과 다른 값으로** 주고 그것이 실제로 반영되는지 본다.
 */
@DisplayName("Kafka 리스너 컨테이너 계약")
class KafkaListenerContractTest {

    private ApplicationContextRunner runner() {
        return new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
            .withUserConfiguration(KafkaConfig.class)
            .withBean(MeterRegistry.class, SimpleMeterRegistry::new);
    }

    @Test
    @DisplayName("application.yml의 listener 설정이 커스텀 팩토리에 반영된다")
    void yml_설정이_팩토리에_반영된다() {
        // 기본값(3)과 다른 값을 준다 — 하드코딩돼 있으면 이 단언이 깨진다.
        runner()
            .withPropertyValues(
                "spring.kafka.listener.concurrency=7",
                "spring.kafka.listener.ack-mode=manual")
            .run(context -> {
                ConcurrentKafkaListenerContainerFactory<?, ?> factory =
                    context.getBean("telemetryBatchListenerContainerFactory",
                        ConcurrentKafkaListenerContainerFactory.class);

                assertThat(factory.getContainerProperties().getAckMode())
                    .isEqualTo(ContainerProperties.AckMode.MANUAL);
                assertThat(factory).extracting("concurrency").isEqualTo(7);
            });
    }

    @Test
    @DisplayName("배치 리스너와 커스텀 에러 핸들러는 설정과 무관하게 유지된다")
    void 배치와_에러핸들러는_configurer가_덮어쓰지_못한다() {
        // configurer는 listener.type(기본 single)과 Boot 기본 에러 핸들러를 넣는다.
        // configure() 뒤에 우리 설정을 덮어쓰는 순서가 깨지면 여기서 잡힌다.
        runner()
            .withPropertyValues("spring.kafka.listener.type=single")
            .run(context -> {
                ConcurrentKafkaListenerContainerFactory<?, ?> factory =
                    context.getBean("telemetryBatchListenerContainerFactory",
                        ConcurrentKafkaListenerContainerFactory.class);

                assertThat(factory.isBatchListener()).isTrue();
                // 우리 DefaultErrorHandler(재시도 예산 + DLQ recoverer)여야 한다.
                assertThat(factory.getContainerProperties()).isNotNull();
                assertThat(context).hasSingleBean(org.springframework.kafka.listener.DefaultErrorHandler.class);
            });
    }

    @Test
    @DisplayName("기본 프로파일 값 — ack-mode manual_immediate, concurrency 3")
    void 기본값() {
        // 운영에서 실제로 쓰는 값. 바뀌면 여기서 드러나야 한다.
        runner()
            .withPropertyValues(
                "spring.kafka.listener.ack-mode=manual_immediate",
                "spring.kafka.listener.concurrency=3")
            .run(context -> {
                ConcurrentKafkaListenerContainerFactory<?, ?> factory =
                    context.getBean("telemetryBatchListenerContainerFactory",
                        ConcurrentKafkaListenerContainerFactory.class);

                assertThat(factory.getContainerProperties().getAckMode())
                    .isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
                assertThat(factory).extracting("concurrency").isEqualTo(3);
            });
    }
}
