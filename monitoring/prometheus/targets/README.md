# 동적 스크레이프 대상

여기 놓인 `*.json`을 Prometheus가 `file_sd_configs`로 읽는다. **파일이 없으면 대상이
없는 것으로 처리되고, 있으면 재시작 없이 반영된다.**

`backend-storage.json`은 `scripts/scale-storage.sh`가 만들고 지운다. 저장소에는
커밋하지 않는다(`.gitignore`) — 커밋해두면 프로파일 `scale`을 안 켠 평상시에도
Prometheus에 `up == 0` 대상이 계속 보이고, 그건 **"장애처럼 보이는 정상"**이라
알림 피로의 원인이 된다.

`static_configs`로 하지 않은 이유가 그거다. 대상이 있다가 없다가 하는 것을
설정 파일에 고정으로 적으면 없을 때가 이상하게 보인다.
