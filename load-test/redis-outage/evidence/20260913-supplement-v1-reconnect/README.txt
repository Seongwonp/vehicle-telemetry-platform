# 보충 증거 — v1 실행 두 건(20260913-113206, 20260913-114124)의 Lettuce 재연결 로그

v1 도구는 backend 로그를 남기지 않았다. 두 실행이 끝난 뒤 **정지된(삭제되지 않은) telemetry-backend 컨테이너의
docker 로그**에서 2026-09-13T02:30~02:50Z 구간의 ConnectionWatchdog/ReconnectionHandler 줄만 뽑았다.
원래 실행 디렉터리는 봉인된 manifest를 깨지 않기 위해 건드리지 않고 여기 따로 둔다.

수집: docker logs -t telemetry-backend | grep -E 'ConnectionWatchdog|ReconnectionHandler|Reconnected'
시각: 첫 칸은 docker가 붙인 UTC 타임스탬프다(앱 로그 자체 시각은 초 단위라 버렸다).
비밀: 호스트명·IP·포트만 있다. 비밀번호 없음.

실행별 기동 명령 시각(각 실행 timeline.txt의 start_cmd_ms):
  90초 실행 20260913-113206: 1789266847498 ms = 2026-09-13T02:34:07.498Z
  30초 실행 20260913-114124: 1789267334379 ms = 2026-09-13T02:42:14.379Z
