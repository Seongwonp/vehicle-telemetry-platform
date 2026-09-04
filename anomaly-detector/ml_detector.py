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
from typing import Deque, List, Tuple

import numpy as np
from sklearn.ensemble import IsolationForest

logger = logging.getLogger("ml_detector")

# 이상 감지에 사용할 피처 (순서 고정)
#
# throttle_position은 원래 빠져 있었는데, 채점해보니 그것 때문에 통째로 못 잡는
# 이상 유형이 있었다 — "스로틀을 밟는데 차가 안 나간다"(throttle_no_response)의
# recall이 6.9%로, 다른 복합 패턴(53-92%)과 확연히 달랐다. 스로틀을 안 보면 이 패턴이
# 그냥 공회전과 구별되지 않아서 파라미터를 조정해도 개선되지 않는다(13차 측정, ADR-018).
FEATURES = ["speed", "rpm", "engine_temp", "battery_voltage", "fuel_level", "throttle_position"]


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
        # 이상 판정 임계값. None이면 모델의 offset_(= 학습 분포의 contamination 분위수)을
        # 그대로 쓴다 — 이 경우 데이터에 이상이 있든 없든 **항상 그 비율만큼** 알림이 뜬다.
        # 값을 주면 그 점수보다 낮은 것만 이상으로 본다. 그러면 알림 수가 설정값이 아니라
        # 실제 데이터에 따라 움직인다(15차 측정, ADR-018):
        #
        #   임계값     복합 recall   정상 오탐률   알림 비율
        #   -0.5540      73.2%         1.8%        5.2%
        #   -0.5259      87.3%         4.2%        8.1%
        #   -0.5118      92.7%         6.3%       10.4%
        #   offset_(현재) 67.8%        약 5%        5.1%
        #
        # 기본값을 None으로 둔 이유: 위 숫자는 **이 시뮬레이터의 점수 분포**에서 나온 것이라
        # 그대로 상수로 박으면 특정 워크로드에 과적합된다. 실제 배포에서는 정상 구간
        # 데이터로 같은 스윕(score_ml.py --sweep)을 돌려 직접 정해야 한다.
        self.score_threshold = score_threshold
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
        # 마지막 학습에 쓴 표본 수. 워밍업 중 재학습 판단(2배 조건)에 쓴다.
        self._trained_with = 0

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

        # predict()를 직접 부르지 않는다 — 그러면 score_threshold를 무시하고 항상
        # offset_으로 판정해서 단건 경로와 배치 경로가 다른 답을 낸다.
        return self._score_and_flag([features])[0][0]

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

        return self._score_and_flag(features_list)[0]

    def update_batch_with_scores(self, batch: List[dict]) -> Tuple[List[bool], List[float]]:
        """`update_batch()`와 같은 일을 하되 이상 점수도 함께 돌려준다(측정·튜닝용).

        <p>점수는 `IsolationForest.score_samples()` 값으로, **낮을수록 이상**이다.
        현재 판정은 이 점수를 모델의 `offset_`(= 학습 데이터에서 `contamination` 비율에
        해당하는 분위수)과 비교하는 것과 정확히 같다 — sklearn의 `predict`가
        `score_samples(X) - offset_ < 0`이기 때문이다. 즉 지금 구조는 데이터에 이상이
        있든 없든 **학습 분포의 하위 5%를 찍는다**(ADR-018, 12·14차 측정).

        <p>이 메서드는 그 `offset_`을 우리가 정한 임계값으로 바꾸기 위한 준비다.
        임계값을 하나씩 찍어 부하를 반복하는 대신, 한 번의 측정에서 모든 메시지의 점수를
        받아 오프라인으로 임계값을 스윕하려고 만들었다.

        <p>비용은 `update_batch()`와 같다 — `predict()`가 내부적으로 `score_samples()`를
        부르고 빼기만 하므로, 점수를 직접 받는다고 추가 계산이 생기지 않는다.
        """
        if not batch:
            return [], []

        features_list = [self._extract(data) for data in batch]
        self._buffer.extend(features_list)
        self._samples_since_train += len(features_list)

        if not self.is_trained:
            if len(self._buffer) >= self.min_samples:
                self._train()
            else:
                # 학습 전에는 점수를 매길 모델이 없다. 판정은 전부 정상, 점수는 nan으로
                # 표시해 "0점"과 구분한다(0.0은 실제로 나올 수 있는 점수다).
                return [False] * len(features_list), [float("nan")] * len(features_list)
        elif self._should_retrain():
            self._train()

        return self._score_and_flag(features_list)

    def _score_and_flag(self, features_list: List[List[float]]) -> Tuple[List[bool], List[float]]:
        scores = self.model.score_samples(np.array(features_list))
        offset = self.model.offset_ if self.score_threshold is None else self.score_threshold
        # numpy 타입이 아니라 파이썬 bool/float로 돌려준다 — 호출부에서 그대로 로직·
        # 직렬화에 쓰이는 값이라 numpy 타입이 새어나가지 않게 한다.
        return [bool(s < offset) for s in scores], [float(s) for s in scores]

    def _should_retrain(self) -> bool:
        """건수와 시간 조건을 모두 만족해야 재학습한다(최초 학습에는 적용되지 않는다).

        <p>단, **윈도우가 아직 안 찬 워밍업 구간에는 시간 하한을 적용하지 않는다.**
        표본이 적은 모델은 정상 데이터를 대량으로 이상이라 찍는다 — 실측하면
        `min_samples`(200)건으로 학습한 최초 모델이 **정상 트래픽의 91%를 이상으로**
        판정했다(전부 정상인 부하, ADR-018 15차). 시간 하한을 그대로 걸면 그 모델이
        60초를 버티므로, 파티션당 초당 500건이면 3만 5천 건이 200건짜리 모델의 판정을
        받는다. `retrain_min_seconds`를 넣기 전에는 500건마다 재학습해 1초 만에
        교체되던 것이라, 처리량을 살리려고 넣은 시간 하한이 만든 회귀다.

        <p>워밍업 중에는 **표본이 직전 학습 대비 2배가 될 때마다** 다시 학습한다.
        건수 기준으로 매번 재학습하면 윈도우가 클 때 학습 횟수가 그만큼 늘지만,
        2배 조건이면 횟수가 log 스케일이라(200→2000은 4회, 200→60000은 9회) 비용이
        제한되면서도 모델 품질은 빠르게 올라간다.
        """
        if self._trained_with < self.window_size:
            # 2배가 윈도우를 넘어서는 마지막 단계에서는 "윈도우가 찼는가"로 본다 —
            # 안 그러면 마지막 학습(윈도우 전체)이 영영 일어나지 않고 절반짜리 모델이
            # 시간 하한만큼 그대로 남는다.
            return len(self._buffer) >= min(self._trained_with * 2, self.window_size)
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
        self._trained_with = len(self._buffer)
        self._last_train_at = time.monotonic()
        logger.info(f"Isolation Forest (재)학습 완료 (윈도우 샘플: {len(self._buffer)}개)")

    def _extract(self, data: dict) -> list[float]:
        return [float(data.get(f) or 0.0) for f in FEATURES]

    # ── 상태 저장/복원 (재시작·리밸런싱 대응) ──────────────────────

    def get_state(self) -> bytes:
        """pickle로 직렬화 — 프로세스가 죽어도 학습 상태를 이어갈 수 있게 한다.

        `features`를 함께 저장한다. 버퍼의 각 행과 학습된 모델이 피처 개수·순서에
        묶여 있어서, 피처 목록이 바뀐 뒤 옛 상태를 그대로 실으면 차원이 어긋난 채로
        학습·예측이 돌아간다(조용히 틀린 결과가 나온다). 복원 시 대조할 수 있게 남긴다.
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
        """get_state()로 저장해둔 상태를 복원한다. 형식이 안 맞으면 예외를 그대로 던진다 —
        호출자가 "복원 실패 시 새로 학습" 여부를 결정하게 한다.

        피처 목록이 지금과 다르면(옛 버전이라 아예 없는 경우 포함) 복원을 거부한다.
        차원이 안 맞는 상태로 이어가느니 처음부터 다시 학습하는 게 맞다 —
        `PartitionedMLDetectors._load`가 이 예외를 잡아 새 detector로 넘어간다.
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
        # 옛 상태에는 없던 값 — 버퍼 크기로 대신한다(복원 직후 곧바로 재학습하지
        # 않도록 보수적으로 잡는다).
        self._trained_with = state.get("trained_with", len(self._buffer))
