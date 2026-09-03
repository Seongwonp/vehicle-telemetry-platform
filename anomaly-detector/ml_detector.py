"""
ML 기반 이상 감지 — Isolation Forest
룰로 잡기 어려운 복합적 패턴 이상 감지

버퍼는 고정 크기 슬라이딩 윈도우(deque)로 관리해 무한 증가하지 않는다. 최초 학습
이후에도 새 샘플이 retrain_interval개 쌓일 때마다 현재 윈도우로 재학습해 concept
drift(주행 패턴 변화)에 대응한다 — 재학습은 새 모델을 다 학습한 뒤에만 기존 모델과
교체하므로, 재학습 중에도 탐지가 끊기지 않는다(최초 학습 전까지만 전부 정상으로 간주).

모델 상태(get_state/load_state)는 pickle로 직렬화한다 — 인스턴스가 재시작되거나 Kafka
파티션 리밸런싱으로 다른 프로세스가 파티션을 넘겨받아도 학습 상태를 이어가기 위함이다
(저장/복원 자체는 anomaly_detector.py가 Redis를 통해 담당한다. ADR-018 참고).
"""
import logging
import pickle
import time
from collections import deque
from typing import Deque, List

import numpy as np
from sklearn.ensemble import IsolationForest

logger = logging.getLogger("ml_detector")

# 이상 감지에 사용할 피처 (순서 고정)
FEATURES = ["speed", "rpm", "engine_temp", "battery_voltage", "fuel_level"]


class MLAnomalyDetector:
    """
    Isolation Forest 기반 이상 감지기.
    초기 min_samples개로 학습하고, 이후 retrain_interval개마다 현재 윈도우로 재학습한다.
    """

    def __init__(
        self,
        contamination: float = 0.05,
        min_samples: int = 200,
        window_size: int = 2000,
        retrain_interval: int = 500,
        retrain_min_seconds: float = 60.0,
    ):
        self.contamination = contamination
        self.min_samples = min_samples
        # 윈도우가 min_samples보다 작으면 최초 학습에 쓸 샘플도 못 채우니 최소한 맞춰준다.
        self.window_size = max(window_size, min_samples)
        self.retrain_interval = retrain_interval
        # 재학습 간격에 시간 하한을 둔다. 건수 기준만 쓰면 처리량에 따라 빈도가 폭주한다 —
        # 실측에서 인스턴스당 약 1,500 msg/s일 때 retrain_interval=500이 초당 2.67회
        # 재학습을 유발했고, fit 1회가 207.5ms라 그것만으로 코어의 약 55%를 먹었다.
        # 재학습의 목적인 concept drift(주행 패턴 변화)는 애초에 시간 현상이라
        # 건수보다 시간이 맞는 기준이다.
        self.retrain_min_seconds = retrain_min_seconds
        self._last_train_at = 0.0

        self.model = IsolationForest(
            contamination=contamination,
            n_estimators=100,
            random_state=42,
        )
        self._buffer: Deque[List[float]] = deque(maxlen=self.window_size)
        self.is_trained = False
        self._samples_since_train = 0

    def update(self, data: dict) -> bool:
        """
        데이터를 받아 버퍼에 추가하고, 이상 여부 반환.
        학습 전이면 False 반환 (정상으로 간주).
        """
        features = self._extract(data)
        self._buffer.append(features)
        self._samples_since_train += 1

        if not self.is_trained:
            if len(self._buffer) >= self.min_samples:
                self._train()
            else:
                return False
        elif self._should_retrain():
            self._train()

        prediction = self.model.predict([features])
        return prediction[0] == -1  # -1 = 이상

    def update_batch(self, batch: List[dict]) -> List[bool]:
        """한 파티션에서 받은 메시지 묶음을 처리하고 이상 여부 리스트를 돌려준다.

        <p>`update()`를 메시지마다 부르면 `model.predict()`도 메시지마다 호출된다.
        실측하면 sklearn의 predict는 **샘플 수가 아니라 호출 횟수**가 비용을 지배한다 —
        단건 11.5ms인데 500건을 한 번에 넣어도 16.8ms(건당 0.034ms)다. 그래서 배치당
        predict를 1회로 묶으면 처리량이 수십 배 올라간다(ADR-018 참고. ADR-011에서
        InfluxDB 쓰기를 배치로 묶은 것과 같은 구조의 최적화다).

        <p>단건 경로와의 의미 차이: `update()`는 샘플을 버퍼에 넣고 그 즉시 예측하므로
        배치 중간에 재학습 임계치를 넘으면 그 이후 샘플은 새 모델로 채점된다. 여기서는
        배치 전체를 버퍼에 넣고 재학습을 한 번만 판단한 뒤 같은 모델로 전부 채점한다.
        모델이 바뀌는 건 재학습 때뿐이고 재학습은 윈도우 전체로 도는 것이라, 배치
        경계에서 최대 배치 크기만큼 채점 시점이 밀리는 정도의 차이다.
        """
        if not batch:
            return []

        features_list = [self._extract(data) for data in batch]
        self._buffer.extend(features_list)
        self._samples_since_train += len(features_list)

        if not self.is_trained:
            if len(self._buffer) >= self.min_samples:
                self._train()
            else:
                # 아직 최초 학습도 못 했으면 전부 정상으로 간주한다(단건 경로와 동일).
                return [False] * len(features_list)
        elif self._should_retrain():
            self._train()

        predictions = self.model.predict(np.array(features_list))
        # numpy.bool_이 아니라 파이썬 bool로 돌려준다 — 호출부에서 그대로 로직·직렬화에
        # 쓰이는 값이라 numpy 타입이 새어나가지 않게 한다.
        return [bool(p == -1) for p in predictions]

    def _should_retrain(self) -> bool:
        """건수와 시간 조건을 모두 만족해야 재학습한다(최초 학습에는 적용되지 않는다)."""
        if self._samples_since_train < self.retrain_interval:
            return False
        return (time.monotonic() - self._last_train_at) >= self.retrain_min_seconds

    def _train(self) -> None:
        # 새 모델을 다 학습한 뒤에만 self.model을 교체한다 — 학습 도중 예외가 나도
        # (예: 윈도우 안 값이 전부 동일해 fit이 이상하게 도는 경우) 기존 모델은 그대로
        # 쓸 수 있고, 재학습 자체가 탐지 공백을 만들지 않는다.
        X = np.array(self._buffer)
        new_model = IsolationForest(
            contamination=self.contamination,
            n_estimators=100,
            random_state=42,
        )
        new_model.fit(X)
        self.model = new_model
        self.is_trained = True
        self._samples_since_train = 0
        self._last_train_at = time.monotonic()
        logger.info(f"Isolation Forest (재)학습 완료 (윈도우 샘플: {len(self._buffer)}개)")

    def _extract(self, data: dict) -> list[float]:
        return [float(data.get(f) or 0.0) for f in FEATURES]

    # ── 상태 저장/복원 (재시작·리밸런싱 대응) ──────────────────────

    def get_state(self) -> bytes:
        """pickle로 직렬화 — 프로세스가 죽어도 학습 상태를 이어갈 수 있게 한다."""
        return pickle.dumps({
            "model": self.model,
            "buffer": list(self._buffer),
            "is_trained": self.is_trained,
            "samples_since_train": self._samples_since_train,
        })

    def load_state(self, blob: bytes) -> None:
        """get_state()로 저장해둔 상태를 복원한다. 형식이 안 맞으면 예외를 그대로 던진다 —
        호출자가 "복원 실패 시 새로 학습" 여부를 결정하게 한다."""
        state = pickle.loads(blob)
        self.model = state["model"]
        self._buffer = deque(state["buffer"], maxlen=self.window_size)
        self.is_trained = state["is_trained"]
        self._samples_since_train = state.get("samples_since_train", 0)
