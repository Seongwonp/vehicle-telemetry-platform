"""
ML 기반 이상 감지 — Isolation Forest
룰로 잡기 어려운 복합적 패턴 이상 감지
"""
import logging
import numpy as np
from sklearn.ensemble import IsolationForest

logger = logging.getLogger("ml_detector")

# 이상 감지에 사용할 피처 (순서 고정)
FEATURES = ["speed", "rpm", "engine_temp", "battery_voltage", "fuel_level"]


class MLAnomalyDetector:
    """
    Isolation Forest 기반 이상 감지기.
    초기 N개 샘플로 학습 후, 이후 데이터에 대해 이상 여부 판단.
    """

    def __init__(self, contamination: float = 0.05, min_samples: int = 200):
        self.model = IsolationForest(
            contamination=contamination,
            n_estimators=100,
            random_state=42,
        )
        self.min_samples = min_samples
        self._buffer: list[list[float]] = []
        self.is_trained = False

    def update(self, data: dict) -> bool:
        """
        데이터를 받아 버퍼에 추가하고, 이상 여부 반환.
        학습 전이면 False 반환 (정상으로 간주).
        """
        features = self._extract(data)
        self._buffer.append(features)

        if not self.is_trained and len(self._buffer) >= self.min_samples:
            self._train()

        if not self.is_trained:
            return False

        prediction = self.model.predict([features])
        return prediction[0] == -1  # -1 = 이상

    def _train(self):
        X = np.array(self._buffer)
        self.model.fit(X)
        self.is_trained = True
        logger.info(f"Isolation Forest 학습 완료 (샘플: {len(self._buffer)}개)")

    def _extract(self, data: dict) -> list[float]:
        return [float(data.get(f) or 0.0) for f in FEATURES]
