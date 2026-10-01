# MQTT ACK 경계 작업 — 2026-10-01

## 실행 상태

- 리뷰 원문 정정·보안 문서 갱신: 완료, 미커밋.
- 수정 전 유실 재현: 실제 MQTT/별도 JVM과 통제한 Kafka future 조건에서 자동 ACK 3/3 재전달 없음, manual ACK 3/3 재전달 관찰. 전체 스택 크래시 실험은 남아 있다.
- ACK 1차 구현: Kafka/spool 확인 뒤 직렬 ACK, 실패 시 ACK 금지. 전체 Java 373건 통과. 처리 동시성 감소와 고부하 미검증을 ADR-029에 명시했다.
- 동글: 시리얼 포트 조회 0건. 연결 방식·차량·브리지 접근 정보가 없어 실측 미실행, runbook만 준비.
- 다음: [검증 기록](../verification/2026-10-01-mqtt-ack-boundary.md)의 전체 스택 크래시·목표 부하·재접속 경계 검증 후 동글 실측. 현재 결과로 전체 at-least-once 완료를 선언하지 않는다.

## 리뷰 정정

외부 PORTFOLIO_REVIEW_2026-09-30.md를 포트폴리오/seongwonp.github.io/docs에서 찾아 본문과 반론 반영 절을 정정했다. 반영한 정정:

- timestamp 창으로 replay 일괄 차단하는 처방 철회: 정상 장애 복구 데이터를 버린다.
- 근거 없는 무작위 기준선 30% 추론 철회.
- 판정자 2인 필수 요구 철회. 필요할 때 선택할 검증 방법이다.
- manual ACK는 백엔드 크래시 구간을 줄인다. 장기 장애의 브로커 포화는 별도 문제다.
- 차량이 받는 PUBACK과 백엔드가 보내는 PUBACK을 구분한다.
- RF=1, OTel 부재, ML 비활성은 공개된 범위의 한계이며 최우선 결함으로 묶지 않는다.
- 72.1% 사례의 재연결 지연 진단과 300초 장애 큐 포화 진단을 분리한다.

## 순서와 성공 기준

1. 현재 보안 구현·근거·미검증 경계를 문서로 정리.
2. 수정 전 수신 후 Kafka 완료 전 백엔드 강제 종료 재현. 정상 대조군과 메시지별 결과 보존.
3. 브로커가 살아 있고 동일 client ID·데이터 볼륨 유지 조건에서 ACK 수정. Kafka 또는 spool 성공 전 ACK 금지; DLQ 실패도 성공 처리 금지.
4. 동글 실제 PID·주기·결측·복구 실측. 장비·차량 접근 없이 완료로 표시하지 않음.

## 구현 판단 기준

- MQTT 3.1.1 §4.6 PUBACK 순서 준수. Kafka 완료 순서대로 ACK하면 안 된다.
- 재접속 전 콜백이 새 연결의 packet ID를 잘못 ACK하지 않아야 한다.
- inflight 창·ACK 지연에 따른 처리량을 같은 조건으로 비교.
- spool 실패 시 성공 ACK 금지. 재전달 중복과 유한 큐의 한계 명시.
- spool force/rename은 프로세스 종료 범위. 호스트 전원 차단·디렉터리 메타데이터·디스크 손실 보장 아님.
- [브로커 기본값](../security-report.md)은 버전과 함께 기록. 무조건 늘리는 변경 금지.

[MQTT 명세](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/errata01/os/mqtt-v3.1.1-errata01-os-complete.pdf), [Spring Integration 6.2.4 어댑터](https://github.com/spring-projects/spring-integration/blob/v6.2.4/spring-integration-mqtt/src/main/java/org/springframework/integration/mqtt/inbound/MqttPahoMessageDrivenChannelAdapter.java).
