package com.telemetry.config;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class InfluxDbConfig {

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
        return InfluxDBClientFactory.create(url, token.toCharArray(), influxOrg, bucket);
    }

    /** Kafka offset을 실제 InfluxDB 쓰기 성공 뒤에만 커밋하기 위해 동기 API를 사용한다. */
    @Bean
    public WriteApiBlocking writeApiBlocking(InfluxDBClient influxDBClient) {
        return influxDBClient.getWriteApiBlocking();
    }
}
