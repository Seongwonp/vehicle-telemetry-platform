package com.telemetry.contract;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.telemetry.domain.VehicleTelemetry;
import com.telemetry.influxdb.TelemetryRepository;
import com.telemetry.repository.VehicleRepository;
import com.telemetry.service.TelemetryQueryService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@Testcontainers(disabledWithoutDocker = true)
class InfluxDbContractTest {

    private static final String TOKEN = "contract-test-token-with-sufficient-length";
    private static final String ORG = "contract-org";
    private static final String BUCKET = "contract-bucket";

    @Container
    static final GenericContainer<?> INFLUX = new GenericContainer<>("influxdb:2.7")
        .withExposedPorts(8086)
        .withEnv("DOCKER_INFLUXDB_INIT_MODE", "setup")
        .withEnv("DOCKER_INFLUXDB_INIT_USERNAME", "contract-user")
        .withEnv("DOCKER_INFLUXDB_INIT_PASSWORD", "contract-password-123")
        .withEnv("DOCKER_INFLUXDB_INIT_ORG", ORG)
        .withEnv("DOCKER_INFLUXDB_INIT_BUCKET", BUCKET)
        .withEnv("DOCKER_INFLUXDB_INIT_ADMIN_TOKEN", TOKEN)
        .waitingFor(Wait.forHttp("/health").forStatusCode(200));

    @Test
    void influx27ExecutesProductionFluxAndReturnsWrittenPoint() {
        String url = "http://" + INFLUX.getHost() + ":" + INFLUX.getMappedPort(8086);
        try (InfluxDBClient client = InfluxDBClientFactory.create(
            url, TOKEN.toCharArray(), ORG, BUCKET)) {
            Instant now = Instant.now();
            client.getWriteApiBlocking().writePoint(Point.measurement("vehicle_telemetry")
                .addTag("vehicle_id", "TEST-001")
                .addField("speed", 87.3)
                .time(now, WritePrecision.MS));
            // fleet 쿼리의 last()+pivot이 차량별 **최신** 포인트를 고르는지 — 더 오래된 포인트와 다른 차량을 같이 넣는다.
            client.getWriteApiBlocking().writePoint(Point.measurement("vehicle_telemetry")
                .addTag("vehicle_id", "TEST-001")
                .addField("speed", 12.0)
                .time(now.minusSeconds(60), WritePrecision.MS));
            client.getWriteApiBlocking().writePoint(Point.measurement("vehicle_telemetry")
                .addTag("vehicle_id", "TEST-002")
                .addField("speed", 55.5)
                .time(now.minusSeconds(5), WritePrecision.MS));

            TelemetryQueryService service = new TelemetryQueryService(
                client, mock(VehicleRepository.class), new SimpleMeterRegistry());
            ReflectionTestUtils.setField(service, "bucket", BUCKET);
            ReflectionTestUtils.setField(service, "influxOrg", ORG);

            assertThat(service.getRecent("TEST-001", 10))
                .extracting(response -> response.getSpeed())
                .containsExactly(87.3, 12.0);
            var fleet = service.getLatestByVehicleIds(List.of("TEST-001", "TEST-002", "TEST-NONE"));
            assertThat(fleet).containsOnlyKeys("TEST-001", "TEST-002");
            assertThat(fleet.get("TEST-001").getSpeed()).isEqualTo(87.3);
            assertThat(fleet.get("TEST-002").getSpeed()).isEqualTo(55.5);
        }
    }

    /**
     * 선택 필드(ADR-030)가 없는 포인트를 <b>운영 {@code toPoint}로</b> 쓰고 운영 Flux로 읽는다.
     *
     * <p>지키는 것 세 가지:
     * <ol>
     *   <li>없는 필드는 InfluxDB에 <b>아예 없다</b> — 0으로 쓰이지 않았다(필드 행 수로 확인).</li>
     *   <li>조회 응답에서 그 값은 <b>null</b>이다(0이 아니다).</li>
     *   <li>fleet 최신값({@code last()}+pivot)이 <b>옛 포인트의 연료·전압을 최신 행에 섞지 않는다.</b></li>
     * </ol>
     */
    @Test
    void 선택필드가_없는_포인트는_필드를_쓰지_않고_조회는_null이다() {
        String url = "http://" + INFLUX.getHost() + ":" + INFLUX.getMappedPort(8086);
        try (InfluxDBClient client = InfluxDBClientFactory.create(
            url, TOKEN.toCharArray(), ORG, BUCKET)) {
            Instant now = Instant.now();
            TelemetryRepository repository =
                new TelemetryRepository(client.getWriteApiBlocking(), new SimpleMeterRegistry());

            VehicleTelemetry older = telemetry("OPT-0001", now.minusSeconds(60), 40.0, 12.6);
            VehicleTelemetry latest = telemetry("OPT-0001", now, null, null);
            repository.saveAll(List.of(repository.toPoint(older), repository.toPoint(latest)));

            // 1) 필드 행 수 — 연료·전압은 옛 포인트 1건에만 있다. 최신 포인트에 0으로 들어갔다면 2건이다.
            String countFlux = String.format("""
                from(bucket: "%s")
                  |> range(start: -1h)
                  |> filter(fn: (r) => r._measurement == "vehicle_telemetry" and r.vehicle_id == "OPT-0001")
                  |> filter(fn: (r) => r._field == "fuel_level" or r._field == "battery_voltage" or r._field == "speed")
                  |> count()
                """, BUCKET);
            java.util.Map<String, Long> counts = new java.util.HashMap<>();
            client.getQueryApi().query(countFlux).forEach(t -> t.getRecords().forEach(r ->
                counts.put((String) r.getValueByKey("_field"), ((Number) r.getValue()).longValue())));
            assertThat(counts).containsEntry("speed", 2L)
                .containsEntry("fuel_level", 1L)
                .containsEntry("battery_voltage", 1L);

            TelemetryQueryService service = new TelemetryQueryService(
                client, mock(VehicleRepository.class), new SimpleMeterRegistry());
            ReflectionTestUtils.setField(service, "bucket", BUCKET);
            ReflectionTestUtils.setField(service, "influxOrg", ORG);

            // 2) 최근 조회 — 최신 행은 null, 옛 행은 값 그대로.
            var recent = service.getRecent("OPT-0001", 10);
            assertThat(recent).hasSize(2);
            assertThat(recent.get(0).getSpeed()).isEqualTo(80.0);
            assertThat(recent.get(0).getFuelLevel()).isNull();
            assertThat(recent.get(0).getBatteryVoltage()).isNull();
            assertThat(recent.get(1).getFuelLevel()).isEqualTo(40.0);
            assertThat(recent.get(1).getBatteryVoltage()).isEqualTo(12.6);

            // 3) fleet 최신값 — last()는 필드별이라 연료의 last()는 60초 전 포인트를 가리킨다.
            //    그 값을 최신 행에 섞으면 "지금 연료 40%"로 보인다. null이어야 한다.
            var fleet = service.getLatestByVehicleIds(List.of("OPT-0001"));
            assertThat(fleet.get("OPT-0001").getTimestamp()).isEqualTo(
                Instant.ofEpochMilli(now.toEpochMilli()).toString());
            assertThat(fleet.get("OPT-0001").getSpeed()).isEqualTo(80.0);
            assertThat(fleet.get("OPT-0001").getFuelLevel()).isNull();
            assertThat(fleet.get("OPT-0001").getBatteryVoltage()).isNull();
        }
    }

    private static VehicleTelemetry telemetry(String id, Instant at, Double fuel, Double battery) {
        VehicleTelemetry t = new VehicleTelemetry();
        t.setVehicleId(id);
        t.setTimestamp(at.toString());
        t.setSpeed(80.0);
        t.setRpm(2000.0);
        t.setEngineTemp(90.0);
        t.setThrottlePosition(30.0);
        t.setFuelLevel(fuel);
        t.setBatteryVoltage(battery);
        return t;
    }
}
