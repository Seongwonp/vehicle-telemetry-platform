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
            // force() 뒤에 rename한다. 예전에는 writeString만 하고 rename해서, 전원이 끊기면 이름은 .json인데
            // 내용이 비었거나 잘린 파일이 남을 수 있었다 — 그 파일 하나가 드레인을 영원히 막았다(quarantine 참고).
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
                // 실패한 쓰기의 .tmp를 지운다. 이 메시지는 영속되지 않았으므로 호출자는 실패를 받고 ACK하지 않는다 —
                // 실패를 삼키지 않는다. 연결 끊김 인터럽트가 write/force에 닿으면(ClosedByInterruptException) 0바이트·
                // 내용만 있는 .tmp가 남았고 정리 주체가 없었다(실험 D3, docs/verification/2026-10-08-spool-interrupt-durability.md).
                discardTemporary(temporary, e);
                throw e;
            }
        } catch (IOException e) {
            throw new IllegalStateException("텔레메트리 로컬 spool 저장 실패", e);
        }
    }

    /** .json으로 옮겨지지 않은 자기 .tmp만 지운다. 지우지 못하면 원래 실패에 붙여 남긴다(드레인은 .tmp를 읽지 않는다). */
    static void discardTemporary(Path temporary, Exception failure) {
        try {
            Files.deleteIfExists(temporary);
        } catch (IOException e) {
            failure.addSuppressed(e);
            log.warn("실패한 spool 쓰기의 임시 파일 삭제 실패 path={}", temporary, e);
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

    /**
     * 내용을 해석할 수 없는 spool 파일을 {@code .corrupt}로 옮겨 드레인 대상에서 뺀다. <b>지우지 않는다</b> — 사람이 볼 증거다.
     *
     * <p>이게 없던 때는 해석 실패 파일이 {@link #pending}에 영원히 남아 backlog가 꺼지지 않았고, 그 뒤의
     * <b>모든 메시지가 MQTT 콜백 스레드에서 디스크를 거쳤다</b> — ADR-019의 99.8% 유실을 만든 그 경로다(2026-09-29 리뷰).
     *
     * @return 옮긴 경로. 옮기지 못하면 null — 호출자는 계속 막혀 있다는 뜻이라 크게 남겨야 한다.
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
