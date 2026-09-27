package com.telemetry.contract;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 실제 PostgreSQL에서 마이그레이션 전체와, V4의 <b>기존 데이터 백필</b>을 본다.
 *
 * <p>V4는 자유 문자열이던 {@code vehicles.owner}를 users FK로 바꾼다. 빈 DB에서만 통과하는
 * 마이그레이션은 운영 DB에서 깨진다 — 그래서 V3까지 올린 뒤 옛 모양의 행을 넣고 V4를 돌린다.
 */
@Testcontainers(disabledWithoutDocker = true)
class FlywayPostgresContractTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName("telemetry")
        .withUsername("telemetry")
        .withPassword("contract-password");

    private static Flyway flyway(String target) {
        var config = Flyway.configure()
            .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
            .locations("classpath:db/migration")
            .placeholders(Map.of("admin_username", "admin"));
        if (target != null) config = config.target(target);
        return config.load();
    }

    private static JdbcTemplate jdbc() {
        return new JdbcTemplate(new DriverManagerDataSource(
            POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    }

    @Test
    @DisplayName("V1~V5 전체 적용, event_id 멱등 insert, V4의 소유자 백필이 실제 PostgreSQL에서 동작한다")
    void migrationsAndOwnerBackfillWorkOnPostgres() {
        JdbcTemplate jdbc = jdbc();

        // ── V3까지: 옛 스키마에 자유 문자열 소유자 행을 만든다 ──
        assertThat(flyway("3").migrate().migrationsExecuted).isEqualTo(3);
        jdbc.update("INSERT INTO vehicles(vehicle_id, name, owner) VALUES ('OLD-001', 'a', 'hong')");
        jdbc.update("INSERT INTO vehicles(vehicle_id, name, owner) VALUES ('OLD-002', 'b', 'admin')");
        jdbc.update("INSERT INTO vehicles(vehicle_id, name, owner) VALUES ('OLD-003', 'c', NULL)");

        // ── V4 ──
        assertThat(flyway(null).migrate().migrationsExecuted).isEqualTo(2);

        // 관리자는 활성 ADMIN, 백필된 hong은 로그인 불가(비활성·해시 없음)
        assertThat(jdbc.queryForMap("SELECT role, active, password_hash FROM users WHERE username = 'admin'"))
            .containsEntry("role", "ADMIN").containsEntry("active", true).containsEntry("password_hash", null);
        assertThat(jdbc.queryForMap("SELECT role, active FROM users WHERE username = 'hong'"))
            .containsEntry("role", "USER").containsEntry("active", false);

        // owner_id가 이름대로 연결되고, NULL 소유자는 관리자로 간다
        assertThat(jdbc.queryForObject("""
            SELECT u.username FROM vehicles v JOIN users u ON u.id = v.owner_id WHERE v.vehicle_id = 'OLD-001'
            """, String.class)).isEqualTo("hong");
        assertThat(jdbc.queryForObject("""
            SELECT u.username FROM vehicles v JOIN users u ON u.id = v.owner_id WHERE v.vehicle_id = 'OLD-003'
            """, String.class)).isEqualTo("admin");

        // 옛 컬럼은 사라지고 FK가 있다 — 존재하지 않는 사용자 소유는 스키마가 막는다
        assertThat(jdbc.queryForObject("""
            SELECT count(*) FROM information_schema.columns
            WHERE table_name = 'vehicles' AND column_name = 'owner'
            """, Integer.class)).isZero();
        assertThat(jdbc.queryForObject("""
            SELECT count(*) FROM pg_constraint WHERE conname = 'fk_vehicles_owner'
            """, Integer.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("""
            SELECT count(*) FROM pg_indexes WHERE indexname IN ('idx_anomaly_vehicle_detected_at', 'idx_vehicles_owner_active', 'idx_anomaly_vehicle_high')
            """, Integer.class)).isEqualTo(3);

        // ── 기존 계약: event_id 멱등 insert ──
        String sql = """
            INSERT INTO anomaly_alerts(event_id, vehicle_id, anomaly_type, detected_at)
            VALUES (?, ?, ?, ?) ON CONFLICT (event_id) DO NOTHING
            """;
        jdbc.update(sql, "a".repeat(64), "TEST-001", "TEST", Timestamp.from(Instant.now()));
        jdbc.update(sql, "a".repeat(64), "TEST-001", "TEST", Timestamp.from(Instant.now()));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM anomaly_alerts", Long.class)).isEqualTo(1L);
    }
}
