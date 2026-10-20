"""
ML 기반 이상 감지 — Isolation Forest. 룰로 잡기 어려운 복합 패턴용.

슬라이딩 윈도우로 주기적으로 재학습하고, 새 모델이 완성된 뒤에만 교체한다.
상태(get_state/load_state)는 재시작·리밸런싱 뒤 이어받기 위한 것이다(ADR-018).
"""
import logging
import pickle
import time
from collections import deque
from typing import Deque, List, Tuple

import numpy as np
from sklearn.ensemble import IsolationForest

logger = logging.getLogger("ml_detector")

# 순서 고정(저장 상태와 묶여 있다). throttle_position을 빼면 throttle_no_response를 못 잡는다(ADR-018).
FEATURES = ["speed", "rpm", "engine_temp", "battery_voltage", "fuel_level", "throttle_position"]


def missing_features(data: dict) -> list[str]:
    """없거나 null인 피처 이름. 비어 있어야 ML에 넣을 수 있다.

    선택 필드(ADR-030)가 빠진 레코드는 ML에서 건너뛴다. 0이나 대체값으로 채우면 이상으로 찍히고
    다음 학습 분포까지 오염된다. 룰 판정은 그대로 돈다.
    """
    return [f for f in FEATURES if data.get(f) is None]


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
        score_threshold: float | None = None,
    ):
        self.contamination = contamination
        # None이면 offset_을 써서 데이터와 무관하게 contamination 비율만큼 알림이 뜬다.
        # 기본값을 상수로 두지 않는 건 시뮬레이터 분포에 과적합되기 때문이다(ADR-018).
        self.score_threshold = score_threshold
        self.min_samples = min_samples
        self.window_size = max(window_size, min_samples)
        self.retrain_interval = retrain_interval
        # 건수 기준만 쓰면 처리량이 오를 때 재학습이 폭주한다 — 시간 하한을 둔다(ADR-018).
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
        # 워밍업 중 2배 조건 판단용
        self._trained_with = 0
        # 관측용이라 상태 저장에는 넣지 않는다.
        self.skipped_missing = 0

    def update(self, data: dict) -> bool:
        """한 건을 버퍼에 넣고 이상 여부를 돌려준다. 학습 전이거나 피처가 빠지면 False."""
        if missing_features(data):
            self.skipped_missing += 1
            return False
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

        # predict()는 score_threshold를 무시하므로 쓰지 않는다(배치 경로와 답이 달라진다).
        return self._score_and_flag([features])[0][0]

    def update_batch(self, batch: List[dict]) -> List[bool]:
        """파티션 배치를 한 번에 채점한다.

        sklearn 채점은 샘플 수보다 호출 횟수가 비용을 좌우해서 배치당 1회로 묶는다(ADR-018).
        단건 경로와 달리 재학습 판단이 배치당 한 번이라 모델 교체가 배치 크기만큼 늦을 수 있다.
        """
        if not batch:
            return []
        return self._process_batch(batch)[0]

    def update_batch_with_scores(self, batch: List[dict]) -> Tuple[List[bool], List[float]]:
        """update_batch()에 score_samples() 점수(낮을수록 이상)를 더해 돌려준다.

        오프라인 임계값 스윕(score_ml.py --sweep)용이며 추가 계산 비용은 없다.
        """
        if not batch:
            return [], []
        return self._process_batch(batch)

    def _process_batch(self, batch: List[dict]) -> Tuple[List[bool], List[float]]:
        """배치 경로 본체. 결과는 입력 순서 그대로다.

        피처가 빠진 레코드(ADR-030)와 학습 전 레코드는 False·nan이다. nan은 "채점 안 함"이라
        실제 점수 0.0과 구분된다.
        """
        flags = [False] * len(batch)
        scores = [float("nan")] * len(batch)
        complete = [i for i, data in enumerate(batch) if not missing_features(data)]
        self.skipped_missing += len(batch) - len(complete)
        if not complete:
            return flags, scores

        features_list = [self._extract(batch[i]) for i in complete]
        self._buffer.extend(features_list)
        self._samples_since_train += len(features_list)

        if not self.is_trained:
            if len(self._buffer) >= self.min_samples:
                self._train()
            else:
                return flags, scores
        elif self._should_retrain():
            self._train()

        scored_flags, scored = self._score_and_flag(features_list)
        for i, flag, score in zip(complete, scored_flags, scored):
            flags[i] = flag
            scores[i] = score
        return flags, scores

    def _score_and_flag(self, features_list: List[List[float]]) -> Tuple[List[bool], List[float]]:
        scores = self.model.score_samples(np.array(features_list))
        offset = self.model.offset_ if self.score_threshold is None else self.score_threshold
        # numpy 타입이 호출부 직렬화로 새지 않게 파이썬 타입으로 바꾼다.
        return [bool(s < offset) for s in scores], [float(s) for s in scores]

    def _should_retrain(self) -> bool:
        """건수와 시간 조건을 모두 만족해야 재학습한다.

        워밍업(윈도우 미충족) 중에는 시간 하한을 걸지 않는다 — 표본이 적은 초기 모델은 정상
        트래픽 대부분을 이상으로 찍으므로 오래 두면 안 된다(ADR-018). 대신 표본이 2배가 될
        때마다 재학습해 횟수를 log 스케일로 제한한다.
        """
        if self._trained_with < self.window_size:
            # 마지막 단계는 "윈도우가 찼는가"로 본다 — 안 그러면 윈도우 전체 학습이 일어나지 않는다.
            return len(self._buffer) >= min(self._trained_with * 2, self.window_size)
        if self._samples_since_train < self.retrain_interval:
            return False
        return (time.monotonic() - self._last_train_at) >= self.retrain_min_seconds

    def _train(self) -> None:
        # fit이 끝난 뒤에만 교체한다 — 학습이 실패해도 기존 모델로 탐지가 이어진다.
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
        self._trained_with = len(self._buffer)
        self._last_train_at = time.monotonic()
        logger.info(f"Isolation Forest (재)학습 완료 (윈도우 샘플: {len(self._buffer)}개)")

    def _extract(self, data: dict) -> list[float]:
        # 0으로 채우지 않는다(ADR-030). 호출자가 먼저 걸렀어야 하므로 여기 오면 버그다.
        missing = missing_features(data)
        if missing:
            raise ValueError(f"ML 피처 없음 {missing} — 0으로 채우지 않는다(missing_features로 먼저 거른다)")
        return [float(data[f]) for f in FEATURES]

    # 상태 저장/복원

    def get_state(self) -> bytes:
        """학습 상태를 pickle로 직렬화한다.

        features를 함께 저장한다 — 피처 목록이 바뀐 뒤 옛 상태를 실으면 차원이 어긋난 채
        조용히 틀린 결과가 나오므로 복원 때 대조한다.
        """
        return pickle.dumps({
            "features": list(FEATURES),
            "model": self.model,
            "buffer": list(self._buffer),
            "is_trained": self.is_trained,
            "samples_since_train": self._samples_since_train,
            "trained_with": self._trained_with,
        })

    def load_state(self, blob: bytes) -> None:
        """get_state() 상태를 복원한다. 형식이나 피처 목록이 다르면 예외를 던지고,
        호출자(PartitionedMLDetectors._load)가 새로 학습한다.
        """
        state = pickle.loads(blob)
        saved_features = state.get("features")
        if saved_features != list(FEATURES):
            raise ValueError(
                f"저장된 피처 목록이 현재와 다르다 — 복원 불가 "
                f"(저장됨: {saved_features}, 현재: {list(FEATURES)})"
            )
        self.model = state["model"]
        self._buffer = deque(state["buffer"], maxlen=self.window_size)
        self.is_trained = state["is_trained"]
        self._samples_since_train = state.get("samples_since_train", 0)
        # 옛 상태에는 없다 — 복원 직후 재학습하지 않도록 버퍼 크기로 대신한다.
        self._trained_with = state.get("trained_with", len(self._buffer))
