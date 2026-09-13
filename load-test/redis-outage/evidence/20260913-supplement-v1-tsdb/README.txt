# 보충 증거 — v1 실행 두 건의 Prometheus TSDB 원본 샘플

v1 도구의 alerts.csv는 약 15~25초 간격 폴링이라 pending·firing 시각이 거칠다.
Prometheus는 규칙을 평가할 때마다 ALERTS 시계열에 샘플을 쓰므로, 그 원본 샘플 시각이
**평가 시각 그 자체**다. 카운터도 scrape 시각의 원본 샘플로 받았다.

수집(2026-09-13, prometheus-data volume에 보존된 TSDB에서):
  GET /api/v1/query  query=ALERTS{alertname=~"RateLimitFailingOpen|RedisUnavailableRejections"}[20m]  time=1789267800
  GET /api/v1/query  query={__name__=~"telemetry_(ratelimit_failopen|redis_unavailable).*"}[20m]      time=1789267800
time=1789267800 = 2026-09-13T02:50:00Z, 범위는 02:30:00Z~02:50:00Z. 두 v1 실행이 모두 들어간다.

실행별 기준 시각(각 실행 timeline.txt):
  90초 20260913-113206: stop_done 1789266757106 (02:32:37.106Z), start_cmd 1789266847498, start_done 1789266850423
  30초 20260913-114124: stop_done 1789267304129 (02:41:44.129Z), start_cmd 1789267334379, start_done 1789267337065
규칙: 수집 당시 monitoring/prometheus/alerts.yml — increase(...[5m]) > 0, for: 1m
