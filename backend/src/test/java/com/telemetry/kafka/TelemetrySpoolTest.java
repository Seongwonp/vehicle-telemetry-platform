package com.telemetry.kafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetrySpoolTest {

    @TempDir
    Path tempDirectory;

    @Test
    void persistsPayloadUntilKafkaAcknowledgesDeletion() {
        TelemetrySpool spool = new TelemetrySpool(tempDirectory.toString());
        Path stored = spool.store("{\"vehicle_id\":\"SIM-001\"}");

        assertThat(spool.pending(10)).containsExactly(stored);
        assertThat(spool.read(stored)).contains("SIM-001");

        spool.delete(stored);
        assertThat(spool.pending(10)).isEmpty();
    }

    @Test
    void quarantineMovesFileOutOfPendingWithoutDeletingIt() {
        TelemetrySpool spool = new TelemetrySpool(tempDirectory.toString());
        Path stored = spool.store("not-json");

        Path moved = spool.quarantine(stored);

        assertThat(spool.pending(10)).isEmpty();
        assertThat(spool.depth()).isZero();
        assertThat(moved).exists().hasFileName(stored.getFileName() + ".corrupt");
        assertThat(spool.read(moved)).isEqualTo("not-json");
    }
}
