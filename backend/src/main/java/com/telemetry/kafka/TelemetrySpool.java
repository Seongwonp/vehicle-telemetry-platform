package com.telemetry.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class TelemetrySpool {

    private final Path spoolDirectory;
    private final AtomicLong sequence = new AtomicLong();
    private final AtomicLong lastScanNanos = new AtomicLong();

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
            // force() 뒤에 rename한다. 아니면 전원 차단 시 비었거나 잘린 .json이 남아 드레인을 막는다.
            FileChannel channel = FileChannel.open(temporary,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
            try {
                try (channel) {
                    ByteBuffer bytes = ByteBuffer.wrap(payload.getBytes(StandardCharsets.UTF_8));
                    while (bytes.hasRemaining()) channel.write(bytes);
                    channel.force(true);
                }
                try {
                    return Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
                } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                    return Files.move(temporary, target);
                }
            } catch (IOException | RuntimeException e) {
                // 남은 .tmp를 지우고 실패를 그대로 던진다 — 영속되지 않았으니 호출자가 ACK하면 안 된다.
                // 경위: docs/verification/2026-10-08-spool-interrupt-durability.md
                discardTemporary(temporary, e);
                throw e;
            }
        } catch (IOException e) {
            throw new IllegalStateException("텔레메트리 로컬 spool 저장 실패", e);
        }
    }

    /** 지우지 못하면 원래 실패에 suppressed로 붙인다(드레인은 .tmp를 읽지 않는다). */
    static void discardTemporary(Path temporary, Exception failure) {
        try {
            Files.deleteIfExists(temporary);
        } catch (IOException e) {
            failure.addSuppressed(e);
            log.warn("실패한 spool 쓰기의 임시 파일 삭제 실패 path={}", temporary, e);
        }
    }

    /**
     * 파일명 순으로 최대 {@code limit}개. 정렬을 빼면 안 된다 — 파일명(밀리초-시퀀스-uuid) 순서가
     * 곧 차량별 메시지 순서다. limit과 무관하게 전체를 정렬하므로 호출 횟수를 줄이는 편이 낫다.
     */
    public List<Path> pending(int limit) {
        long startedAt = System.nanoTime();
        try {
            Files.createDirectories(spoolDirectory);
            try (var paths = Files.list(spoolDirectory)) {
                List<Path> found = paths
                    .filter(path -> path.getFileName().toString().endsWith(".json"))
                    .sorted().limit(limit).toList();
                lastScanNanos.set(System.nanoTime() - startedAt);
                return found;
            }
        } catch (IOException e) {
            throw new IllegalStateException("텔레메트리 spool 조회 실패", e);
        }
    }

    /** 드레인 병목 진단용(스캔 vs 전송). */
    public long lastScanNanos() {
        return lastScanNanos.get();
    }

    /** 게이지용이라 정렬하지 않는다. */
    public long depth() {
        try {
            Files.createDirectories(spoolDirectory);
            try (var paths = Files.list(spoolDirectory)) {
                return paths.filter(path -> path.getFileName().toString().endsWith(".json")).count();
            }
        } catch (IOException e) {
            log.warn("spool 깊이 조회 실패", e);
            return -1;
        }
    }

    public String read(Path path) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("텔레메트리 spool 읽기 실패", e);
        }
    }

    /**
     * 해석 불가 파일을 {@code .corrupt}로 옮겨 드레인에서 뺀다. 지우지 않는다 — 사람이 볼 증거다.
     * 남겨두면 backlog가 꺼지지 않아 모든 새 메시지가 디스크를 거친다(ADR-019).
     *
     * @return 옮긴 경로. 실패하면 null — 드레인이 계속 막혀 있다는 뜻이다.
     */
    public Path quarantine(Path path) {
        Path target = path.resolveSibling(path.getFileName() + ".corrupt");
        try {
            return Files.move(path, target, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log.error("spool 손상 파일 격리 실패 — 드레인이 계속 막힌다 path={}", path, e);
            return null;
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
