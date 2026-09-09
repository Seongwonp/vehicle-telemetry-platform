package com.telemetry.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.telemetry.domain.TelemetryDecoder;
import jakarta.validation.Validation;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 테스트가 <b>운영과 같은 decoder</b>를 쓰게 한다.
 *
 * <p>테스트마다 {@code new ObjectMapper()}로 decoder를 만들면 운영과 다른 것을 검증하게 된다 —
 * 실제로 그 차이 때문에 결정표의 한 칸이 틀린 적이 있다(2026-09-09,
 * {@code FAIL_ON_UNKNOWN_PROPERTIES}가 raw 매퍼에서는 켜져 있고 Boot 매퍼에서는 꺼져 있다).
 * 여기서 <b>Boot 자동 구성 매퍼</b>를 꺼내 {@link TelemetryDecoder}에 그대로 넘긴다.
 */
public final class TestDecoders {

    private static final ObjectMapper BOOT_MAPPER = bootMapper();

    private TestDecoders() { }

    /** 앱이 실제로 쓰는 매퍼. */
    public static ObjectMapper bootObjectMapper() {
        return BOOT_MAPPER;
    }

    /** 운영과 같은 설정의 telemetry decoder. */
    /**
     * <b>측정 전용</b> — P0-2a 이전의 decode 경로({@code readValue} 한 번)를 재현한다.
     *
     * <p>단계적 파싱으로 바꾸면서 trailing JSON·중복 필드 정책이 의도치 않게 바뀌지
     * 않았는지 <b>나란히 놓고</b> 비교하려고 둔다. 운영 코드는 이걸 쓰지 않는다.
     */
    public static ObjectMapper strictMapperLikeBefore() {
        return bootObjectMapper().copy()
            .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    public static TelemetryDecoder telemetryDecoder() {
        return new TelemetryDecoder(
            BOOT_MAPPER,
            Validation.buildDefaultValidatorFactory().getValidator());
    }

    private static ObjectMapper bootMapper() {
        AtomicReference<ObjectMapper> ref = new AtomicReference<>();
        new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(JacksonAutoConfiguration.class))
            .run(context -> ref.set(context.getBean(ObjectMapper.class)));
        return ref.get();
    }
}
