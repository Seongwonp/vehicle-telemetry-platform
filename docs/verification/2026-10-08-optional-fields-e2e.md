# 선택 필드(연료량·전압) 실스택 E2E — 2026-10-08

상태: **1회 관찰.** ADR-030(`86d7335`)을 이미지로 다시 빌드한 로컬 dev 스택에서 MQTT → Kafka → InfluxDB / 감지기 → PostgreSQL을 한 번 확인했다.
원본: [`evidence/2026-10-08-optional-fields-e2e/`](evidence/2026-10-08-optional-fields-e2e/).

## 입력 (차량 `OPTF-E2E`, MQTT QoS 1, 3건)

| # | fuel_level | battery_voltage | 기대 |
| --- | --- | --- | --- |
| 1 | 키 없음 | 키 없음 | 저장, 두 필드 미기록, 저전압 판정 안 함 |
| 2 | `null` | `null` | 1과 같음 |
| 3 | 55.0 | 10.5 | 저장, 두 필드 기록, 저전압 알림 1건 |

## 결과

- PUBACK 3/3.
- InfluxDB 필드별 행 수: `speed`·`rpm`·`engine_temp`·`throttle_position` **3**, `fuel_level`·`battery_voltage` **1** — 없는 값이 0으로 쓰이지 않았다(`influx_field_counts.txt`).
- 감지기: `배터리 저전압` 알림 **1건**(#3, 10.5V)만 PostgreSQL에 저장. #1·#2는 알림 없음(`anomaly_alerts.txt`, `logs_grep.txt`).
- 백엔드·감지기 로그에 거부·DLQ·ERROR 없음(해당 구간).

## 한계

- 1회, 차량 1대, 3건. 앱 화면('미수신' 표시)은 위젯 테스트로만 확인했고 이 스택에서 보지 않았다.
- ML 경로(필드 누락 시 점수 생략)는 이 스택 설정에서 따로 확인하지 않았다 — 단위·소비 루프 테스트 기준.
- 남긴 데이터: InfluxDB `OPTF-E2E` 3행, `anomaly_alerts` 1행.
