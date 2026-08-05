package com.telemetry.contract;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class FlywayPostgresContractTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName("telemetry")
        .withUsername("telemetry")
        .withPassword("contract-password");

    @Test
    void flywayV1ToV3AndIdempotentInsertWorkOnPostgres() {
        Flyway flyway = Flyway.configure()
            .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
            .locations("classpath:db/migration")
            .load();
        assertThat(flyway.migrate().migrationsExecuted).isEqualTo(3);

        JdbcTemplate jdbc = new JdbcTemplate(new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        String sql = """
            INSERT INTO anomaly_alerts(event_id, vehicle_id, anomaly_type, detected_at)
            VALUES (?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING
            """;
        jdbc.update(sql, "a".repeat(64), "TEST-001", "TEST", Timestamp.from(Instant.now()));
        jdbc.update(sql, "a".repeat(64), "TEST-001", "TEST", Timestamp.from(Instant.now()));

        assertThat(jdbc.queryForObject("SELECT count(*) FROM anomaly_alerts", Long.class)).isEqualTo(1L);
        assertThat(jdbc.queryForObject("""
            SELECT count(*) FROM pg_indexes
            WHERE indexname = 'idx_anomaly_vehicle_detected_at'
            """, Integer.class)).isEqualTo(1);
    }
}
