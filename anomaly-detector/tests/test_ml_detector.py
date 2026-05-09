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
