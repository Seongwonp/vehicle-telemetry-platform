package com.telemetry.config;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import com.influxdb.client.WriteApiBlocking;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Slf4j
@Configuration
public class InfluxDbConfig {

    // WriteApiBlocking.writePoint()는 재시도 없이 요청 1건당 1번만 시도한다(InfluxDB Java
    // 클라이언트 7.0.0 바이트코드로 직접 확인 — 재시도/백오프는 비동기 WriteApi 전용이고
    // WriteApiBlocking엔 없다). 이 값을 명시하지 않으면 OkHttpClient 기본값(10초)이
    // 그대로 적용되는데, InfluxDB가 순간적으로(예: shard rotation) 느려질 때 한 배치
    // (Kafka max.poll.records 기본 500건)에서 여러 건이 연달아 타임아웃까지 다 채우면
    // 누적 지연이 max.poll.interval.ms(5분)를 넘겨 컨슈머가 그룹에서 튕겨나간다 —
    // 12시간 soak test에서 실제로 이렇게 재현됐다. 짧게 잡아 개별 실패가 빨리 끝나게 한다.
    private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(5);

    @Value("${influxdb.url}")
    private String url;

    @Value("${influxdb.token}")
    private String token;

    // 필드명을 org로 두면 Lombok @Slf4j가 생성하는 org.slf4j.Logger 참조가
    // 이 인스턴스 필드와 이름이 충돌해 "non-static variable org" 컴파일 에러가 난다.
    @Value("${influxdb.org}")
    private String influxOrg;

    @Value("${influxdb.bucket}")
    private String bucket;

    @Bean
    public InfluxDBClient influxDBClient() {
        InfluxDBClientOptions options = InfluxDBClientOptions.builder()
            .url(url)
            .authenticateToken(token.toCharArray())
            .org(influxOrg)
            .bucket(bucket)
            .okHttpClient(new OkHttpClient.Builder()
                .connectTimeout(WRITE_TIMEOUT)
                .readTimeout(WRITE_TIMEOUT)
                .writeTimeout(WRITE_TIMEOUT))
            .build();
        return InfluxDBClientFactory.create(options);
    }

    /** Kafka offset을 실제 InfluxDB 쓰기 성공 뒤에만 커밋하기 위해 동기 API를 사용한다. */
    @Bean
    public WriteApiBlocking writeApiBlocking(InfluxDBClient influxDBClient) {
        return influxDBClient.getWriteApiBlocking();
    }
}
