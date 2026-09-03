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
        # 이 테스트는 "건수 조건"만 본다 — 시간 하한은 TestRetrainMinSeconds에서 따로 검증한다.
        detector = MLAnomalyDetector(min_samples=10, window_size=200, retrain_interval=20,
                                     retrain_min_seconds=0)
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


# ── update_batch(): 배치 예측 (ADR-018 — predict 호출당 비용이 지배적) ──────

class TestUpdateBatch:

    def test_학습_전에는_전부_정상으로_본다(self):
        detector = MLAnomalyDetector(min_samples=100)
        batch = [make_normal_data(seed=i) for i in range(10)]

        results = detector.update_batch(batch)

        assert results == [False] * 10
        assert detector.is_trained is False

    def test_배치_크기만큼_결과를_돌려준다(self):
        detector = MLAnomalyDetector(min_samples=50)
        batch = [make_normal_data(seed=i) for i in range(60)]

        results = detector.update_batch(batch)

        assert len(results) == 60
        assert all(isinstance(r, bool) for r in results)
        assert detector.is_trained is True

    def test_빈_배치는_빈_결과(self):
        detector = MLAnomalyDetector(min_samples=10)
        assert detector.update_batch([]) == []

    def test_버퍼에_배치_전체가_들어간다(self):
        detector = MLAnomalyDetector(min_samples=1_000_000, window_size=1_000_000)
        detector.update_batch([make_normal_data(seed=i) for i in range(30)])

        assert len(detector._buffer) == 30

    def test_단건_경로와_같은_판정을_낸다(self):
        # 같은 데이터를 같은 순서로 넣으면(학습 시점만 다를 뿐) 동일 모델이 되므로
        # 판정도 같아야 한다 — 배치화가 탐지 결과를 바꾸지 않음을 확인한다.
        warmup = [make_normal_data(seed=i) for i in range(60)]
        probe = make_normal_data(seed=9999)

        one_by_one = MLAnomalyDetector(min_samples=50, retrain_interval=1_000_000)
        for d in warmup:
            one_by_one.update(d)
        expected = one_by_one.update(probe)

        batched = MLAnomalyDetector(min_samples=50, retrain_interval=1_000_000)
        batched.update_batch(warmup)
        actual = batched.update_batch([probe])[0]

        assert actual == expected

    def test_재학습_임계치를_넘으면_배치_안에서_한_번_재학습한다(self):
        # 건수 조건만 보기 위해 시간 하한은 끈다.
        detector = MLAnomalyDetector(min_samples=10, retrain_interval=20,
                                     retrain_min_seconds=0)
        detector.update_batch([make_normal_data(seed=i) for i in range(10)])
        assert detector.is_trained is True

        trained_model = detector.model
        # 임계치(20)를 넘는 배치를 넣으면 재학습이 일어나 모델 객체가 교체된다.
        detector.update_batch([make_normal_data(seed=100 + i) for i in range(25)])

        assert detector.model is not trained_model
        assert detector._samples_since_train == 0


# ── 재학습 시간 하한 (ADR-018 — 건수 기준만으로는 처리량에 따라 빈도가 폭주) ──

class TestRetrainMinSeconds:

    def test_시간_하한_전에는_건수를_넘겨도_재학습하지_않는다(self):
        detector = MLAnomalyDetector(min_samples=10, retrain_interval=20,
                                     retrain_min_seconds=3600)
        detector.update_batch([make_normal_data(seed=i) for i in range(10)])
        trained_model = detector.model

        # 건수 조건(20)은 한참 넘겼지만 시간 하한(1시간)에 걸려 재학습되면 안 된다.
        detector.update_batch([make_normal_data(seed=100 + i) for i in range(100)])

        assert detector.model is trained_model

    def test_시간_하한이_0이면_건수만으로_재학습한다(self):
        detector = MLAnomalyDetector(min_samples=10, retrain_interval=20,
                                     retrain_min_seconds=0)
        detector.update_batch([make_normal_data(seed=i) for i in range(10)])
        trained_model = detector.model

        detector.update_batch([make_normal_data(seed=100 + i) for i in range(25)])

        assert detector.model is not trained_model

    def test_최초_학습은_시간_하한의_영향을_받지_않는다(self):
        # 시간 하한은 재학습에만 적용된다 — 최초 학습까지 지연되면 그동안 탐지가 비어버린다.
        detector = MLAnomalyDetector(min_samples=10, retrain_min_seconds=3600)

        detector.update_batch([make_normal_data(seed=i) for i in range(10)])

        assert detector.is_trained is True
