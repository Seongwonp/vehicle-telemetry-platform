package com.telemetry.config;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;
import com.influxdb.client.write.events.WriteErrorEvent;
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

    /**
     * 비동기 배치 WriteApi. 차량 수가 늘어날수록 메시지마다 개별 HTTP 요청을 보내는
     * WriteApiBlocking은 InfluxDB에 부하가 크다 — 내부 버퍼에 포인트를 모았다가
     * batchSize 또는 flushInterval 조건에 도달하면 한 번에 flush한다.
     * WriteApi는 내부에 백그라운드 플러시 스레드/버퍼를 갖고 있어 애플리케이션당 하나만
     * 유지해야 하므로 싱글턴 빈으로 등록하고, 종료 시 close()로 남은 버퍼를 flush한다.
     */
    @Bean(destroyMethod = "close")
    public WriteApi writeApi(InfluxDBClient influxDBClient) {
        WriteOptions writeOptions = WriteOptions.builder()
            .batchSize(500)
            .flushInterval(1000)
            .build();

        WriteApi writeApi = influxDBClient.makeWriteApi(writeOptions);
        writeApi.listenEvents(WriteErrorEvent.class, event ->
            log.error("[InfluxDB] 배치 쓰기 실패", event.getThrowable()));
        return writeApi;
    }
}
