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

    // OkHttp 기본값(10초)이면 InfluxDB가 잠깐 느려질 때 한 배치의 타임아웃이 누적돼
    // max.poll.interval.ms를 넘기고 컨슈머가 그룹에서 빠진다. 짧게 잡아 실패를 빨리 끝낸다.
    private static final Duration WRITE_TIMEOUT = Duration.ofSeconds(5);

    @Value("${influxdb.url}")
    private String url;

    @Value("${influxdb.token}")
    private String token;

    // org로 이름 지으면 @Slf4j가 만드는 org.slf4j 참조와 충돌해 컴파일이 깨진다.
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
