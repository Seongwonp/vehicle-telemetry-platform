package com.telemetry.contract;

import com.telemetry.security.RefreshTokenService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class RedisRefreshTokenContractTest {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.2-alpine")
        .withExposedPorts(6379);

    private LettuceConnectionFactory connectionFactory;
    private RefreshTokenService service;

    @BeforeEach
    void setUp() {
        connectionFactory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        StringRedisTemplate template = new StringRedisTemplate(connectionFactory);
        template.afterPropertiesSet();
        service = new RefreshTokenService(template);
    }

    @AfterEach
    void tearDown() {
        connectionFactory.destroy();
    }

    @Test
    void redisGetDelAllowsOnlyOneConcurrentRotation() throws Exception {
        String token = service.issue("admin");
        var executor = Executors.newFixedThreadPool(2);
        try {
            CountDownLatch start = new CountDownLatch(1);
            Callable<Optional<String>> rotate = () -> {
                start.await();
                return service.rotate(token);
            };
            var first = executor.submit(rotate);
            var second = executor.submit(rotate);
            start.countDown();
            List<Optional<String>> results = List.of(first.get(), second.get());
            assertThat(results).filteredOn(Optional::isPresent).hasSize(1);
            assertThat(results).filteredOn(Optional::isEmpty).hasSize(1);
        } finally {
            executor.shutdownNow();
        }
    }
}
