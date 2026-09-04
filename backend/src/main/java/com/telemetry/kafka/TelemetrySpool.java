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

    /**
     * 재전송 대상 파일을 파일명(=시간) 순으로 최대 {@code limit}개 돌려준다.
     *
     * <p><b>비용 주의</b>: `sorted()`가 limit과 무관하게 디렉터리 전체를 훑어 정렬한다.
     * spool에 파일이 수만 개면 이 호출 하나가 비싸지므로, 호출 횟수를 줄이는 쪽
     * (배치를 키우는 쪽)이 주기를 줄이는 쪽보다 유리하다. 정렬을 뺄 수는 없다 —
     * 파일명이 `밀리초-시퀀스-uuid`라 이 순서가 곧 차량별 메시지 순서다.
     *
     * <p>스캔에 걸린 시간과 실제 파일 수를 계측해서 남긴다. 드레인이 느릴 때
     * "스캔이 비싼가, 전송이 느린가"를 추측하지 않고 가를 수 있어야 하기 때문이다
     * (실측: 유입 1,700 msg/s에 드레인 19 msg/s였다 —
     * `load-test/fault-injection/RESULT_20260904_fault_injection.md`).
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

    /** 마지막 {@link #pending(int)} 호출에 걸린 시간(ns). 드레인 병목 진단용. */
    public long lastScanNanos() {
        return lastScanNanos.get();
    }

    /**
     * spool에 남은 파일 수. 게이지로 노출하기 위한 것이라 {@link #pending(int)}와 달리
     * 정렬하지 않는다 — 세기만 하면 되므로 훨씬 싸다.
     */
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

    public void delete(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.error("Kafka 전송 완료 spool 삭제 실패 path={}", path, e);
        }
    }
}
