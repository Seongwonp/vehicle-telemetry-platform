package com.telemetry.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class TelemetrySpool {

    private final Path spoolDirectory;
    private final AtomicLong sequence = new AtomicLong();

    public TelemetrySpool(@Value("${telemetry.spool.path:data/telemetry-spool}") String path) {
        this.spoolDirectory = Path.of(path).toAbsolutePath().normalize();
    }

    public Path store(String payload) {
        try {
            Files.createDirectories(spoolDirectory);
            String id = String.format("%013d-%020d-%s",
                System.currentTimeMillis(), sequence.getAndIncrement(), UUID.randomUUID());
            Path temporary = spoolDirectory.resolve(id + ".tmp");
            Path target = spoolDirectory.resolve(id + ".json");
            Files.writeString(temporary, payload, StandardCharsets.UTF_8);
            try {
                return Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                return Files.move(temporary, target);
            }
        } catch (IOException e) {
            throw new IllegalStateException("텔레메트리 로컬 spool 저장 실패", e);
        }
    }

    public List<Path> pending(int limit) {
        try {
            Files.createDirectories(spoolDirectory);
            try (var paths = Files.list(spoolDirectory)) {
                return paths.filter(path -> path.getFileName().toString().endsWith(".json"))
                    .sorted().limit(limit).toList();
            }
        } catch (IOException e) {
            throw new IllegalStateException("텔레메트리 spool 조회 실패", e);
        }
    }

    public String read(Path path) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("텔레메트리 spool 읽기 실패", e);
        }
    }

    public void delete(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.error("Kafka 전송 완료 spool 삭제 실패 path={}", path, e);
        }
    }
}
