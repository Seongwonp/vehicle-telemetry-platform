package com.telemetry.contract;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
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
            client.getWriteApiBlocking().writePoint(Point.measurement("vehicle_telemetry")
                .addTag("vehicle_id", "TEST-001")
                .addField("speed", 87.3)
                .time(Instant.now(), WritePrecision.MS));

            TelemetryQueryService service = new TelemetryQueryService(
                client, mock(VehicleRepository.class), new SimpleMeterRegistry());
            ReflectionTestUtils.setField(service, "bucket", BUCKET);
            ReflectionTestUtils.setField(service, "influxOrg", ORG);

            assertThat(service.getRecent("TEST-001", 10))
                .singleElement()
                .extracting(response -> response.getSpeed())
                .isEqualTo(87.3);
            assertThat(service.getLatestByVehicleIds(List.of("TEST-001")))
                .containsKey("TEST-001");
        }
    }
}
