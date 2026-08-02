"""
ml_detector.py Isolation Forest 테스트
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from ml_detector import MLAnomalyDetector


def make_normal_data(seed: int = 0) -> dict:
    import random
    random.seed(seed)
    return {
        "speed": random.uniform(60.0, 120.0),
        "rpm": random.randint(1500, 3500),
        "engine_temp": random.uniform(85.0, 98.0),
        "battery_voltage": random.uniform(13.5, 14.2),
        "fuel_level": random.uniform(30.0, 80.0),
    }


class TestMLAnomalyDetector:

    def test_학습전_항상_정상반환(self):
        detector = MLAnomalyDetector(min_samples=100)
        result = detector.update(make_normal_data())
        assert result is False  # 학습 전이므로 False

    def test_최소_샘플_미달_학습안됨(self):
        detector = MLAnomalyDetector(min_samples=100)
        for i in range(50):
            detector.update(make_normal_data(seed=i))
        assert detector.is_trained is False

    def test_최소_샘플_달성_후_학습완료(self):
        detector = MLAnomalyDetector(min_samples=50)
        for i in range(50):
            detector.update(make_normal_data(seed=i))
        assert detector.is_trained is True

    def test_학습후_정상데이터_이상아님(self):
        detector = MLAnomalyDetector(min_samples=50)
        # 정상 데이터로 학습
        for i in range(50):
            detector.update(make_normal_data(seed=i))

        # 학습 후 정상 데이터는 대체로 정상으로 분류되어야 함
        normal_results = [detector.update(make_normal_data(seed=i+100)) for i in range(20)]
        # contamination=0.05이므로 5% 이하만 이상으로 분류되어야 함
        anomaly_rate = sum(normal_results) / len(normal_results)
        assert anomaly_rate <= 0.2  # 20% 이하 (테스트 환경 여유치)

    def test_버퍼에_샘플_누적(self):
        detector = MLAnomalyDetector(min_samples=100)
        for i in range(10):
            detector.update(make_normal_data(seed=i))
        assert len(detector._buffer) == 10

    def test_버퍼는_window_size를_넘지_않음(self):
        # 예전 버전은 리스트라 학습 이후에도 계속 append돼 무한히 커졌다 — 이제는
        # window_size로 상한을 둔 슬라이딩 윈도우라 넘지 않아야 한다.
        detector = MLAnomalyDetector(min_samples=10, window_size=50, retrain_interval=1_000_000)
        for i in range(200):
            detector.update(make_normal_data(seed=i))
        assert len(detector._buffer) == 50

    def test_retrain_interval_도달하면_재학습(self):
        detector = MLAnomalyDetector(min_samples=10, window_size=200, retrain_interval=20)
        for i in range(10):
            detector.update(make_normal_data(seed=i))
        assert detector.is_trained is True
        model_after_initial_train = detector.model

        for i in range(20):
            detector.update(make_normal_data(seed=i + 100))

        # retrain_interval(20)을 채웠으니 모델 객체 자체가 교체됐어야 한다.
        assert detector.model is not model_after_initial_train
        assert detector._samples_since_train == 0

    def test_재학습_전에는_모델_그대로(self):
        detector = MLAnomalyDetector(min_samples=10, window_size=200, retrain_interval=20)
        for i in range(10):
            detector.update(make_normal_data(seed=i))
        model_after_initial_train = detector.model

        for i in range(19):  # retrain_interval(20) 미달
            detector.update(make_normal_data(seed=i + 100))

        assert detector.model is model_after_initial_train

    def test_상태_저장후_복원하면_동일하게_판정(self):
        detector = MLAnomalyDetector(min_samples=30, window_size=200)
        for i in range(30):
            detector.update(make_normal_data(seed=i))
        assert detector.is_trained is True

        state = detector.get_state()

        restored = MLAnomalyDetector(min_samples=30, window_size=200)
        assert restored.is_trained is False  # 복원 전에는 미학습 상태
        restored.load_state(state)

        assert restored.is_trained is True
        assert len(restored._buffer) == len(detector._buffer)
        # 같은 입력에 같은 판정을 내려야 한다(모델 자체를 그대로 복원했으므로).
        sample = make_normal_data(seed=999)
        assert restored.update(sample) == detector.update(sample)

    def test_복원된_버퍼도_window_size_상한을_지킴(self):
        detector = MLAnomalyDetector(min_samples=10, window_size=200)
        for i in range(10):
            detector.update(make_normal_data(seed=i))
        state = detector.get_state()

        # 더 작은 window_size로 복원해도 deque maxlen이 새 값으로 재설정돼야 한다
        # (min_samples 이상으로 클램프되니 둘 다 5로 맞춘다).
        restored = MLAnomalyDetector(min_samples=5, window_size=5)
        restored.load_state(state)
        assert restored._buffer.maxlen == 5
