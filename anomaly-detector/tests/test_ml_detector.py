"""
ml_detector.py Isolation Forest 테스트
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pickle

import pytest
from sklearn.ensemble import IsolationForest

import ml_detector
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
        # 정상 주행에서 스로틀은 속도에 대체로 비례한다(시뮬레이터도 그렇게 만든다).
        # 이 값이 없으면 _extract가 0.0으로 채워 피처가 상수가 되므로 테스트가
        # 실제 형태를 반영하지 못한다.
        "throttle_position": random.uniform(20.0, 60.0),
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
        # 윈도우를 min_samples와 같게 둬서 곧바로 워밍업이 끝나게 한다 —
        # 여기서 보려는 건 워밍업이 아니라 정상 구간의 건수 조건이다.
        detector = MLAnomalyDetector(min_samples=10, window_size=10, retrain_interval=20,
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
        detector = MLAnomalyDetector(min_samples=10, window_size=10, retrain_interval=20)
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
        detector = MLAnomalyDetector(min_samples=10, window_size=10, retrain_interval=20,
                                     retrain_min_seconds=3600)
        detector.update_batch([make_normal_data(seed=i) for i in range(10)])
        trained_model = detector.model

        # 건수 조건(20)은 한참 넘겼지만 시간 하한(1시간)에 걸려 재학습되면 안 된다
        # (윈도우가 이미 찼으므로 워밍업 예외에도 걸리지 않는다).
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


# ── 피처 변경 시 상태 호환 (ADR-018 — 차원 불일치로 조용히 틀리는 걸 막는다) ──

class TestStateFeatureCompatibility:

    def test_같은_피처면_복원된다(self):
        detector = MLAnomalyDetector(min_samples=10)
        for i in range(15):
            detector.update(make_normal_data(seed=i))

        restored = MLAnomalyDetector(min_samples=10)
        restored.load_state(detector.get_state())

        assert restored.is_trained is True
        assert len(restored._buffer) == len(detector._buffer)

    def test_피처가_바뀌면_복원을_거부한다(self, monkeypatch):
        detector = MLAnomalyDetector(min_samples=10)
        for i in range(15):
            detector.update(make_normal_data(seed=i))
        blob = detector.get_state()

        # 저장 후 피처가 하나 늘어난 상황을 흉내낸다.
        monkeypatch.setattr(ml_detector, "FEATURES", list(ml_detector.FEATURES) + ["extra"])

        with pytest.raises(ValueError, match="피처 목록"):
            MLAnomalyDetector(min_samples=10).load_state(blob)

    def test_피처_정보가_없는_옛_상태도_거부한다(self):
        # features 키가 없던 시절의 상태 — 5개 피처 기준이라 지금과 차원이 다르다.
        legacy = pickle.dumps({
            "model": IsolationForest(n_estimators=10).fit([[0.0] * 5] * 10),
            "buffer": [[0.0] * 5] * 10,
            "is_trained": True,
            "samples_since_train": 0,
        })

        with pytest.raises(ValueError, match="피처 목록"):
            MLAnomalyDetector(min_samples=10).load_state(legacy)


# ── update_batch_with_scores(): 점수 기반 임계값 재설계 준비 (ADR-018) ────────

class TestUpdateBatchWithScores:

    def test_판정이_update_batch와_완전히_같다(self):
        # 점수를 직접 받아 offset_과 비교하는 것이 predict()와 동일해야 한다 —
        # 다르면 "측정용 경로"와 "실제 판정"이 어긋나 채점 결과를 믿을 수 없다.
        warmup = [make_normal_data(seed=i) for i in range(60)]
        probes = [make_normal_data(seed=500 + i) for i in range(40)]

        plain = MLAnomalyDetector(min_samples=50, retrain_interval=1_000_000)
        plain.update_batch(warmup)
        expected = plain.update_batch(probes)

        scored = MLAnomalyDetector(min_samples=50, retrain_interval=1_000_000)
        scored.update_batch_with_scores(warmup)
        flags, scores = scored.update_batch_with_scores(probes)

        assert flags == expected
        assert len(scores) == len(probes)

    def test_이상으로_찍힌_쪽_점수가_더_낮다(self):
        # 점수는 낮을수록 이상이라는 방향을 고정한다 — 임계값 스윕이 이 방향에 의존한다.
        detector = MLAnomalyDetector(min_samples=50, retrain_interval=1_000_000)
        detector.update_batch_with_scores([make_normal_data(seed=i) for i in range(60)])

        probes = [make_normal_data(seed=500 + i) for i in range(60)]
        probes.append({"speed": 400.0, "rpm": 12000, "engine_temp": 200.0,
                       "battery_voltage": 3.0, "fuel_level": 0.0,
                       "throttle_position": 100.0})
        flags, scores = detector.update_batch_with_scores(probes)

        assert flags[-1] is True  # 노골적인 이상은 잡혀야 한다
        assert scores[-1] < min(scores[:-1])

    def test_학습_전에는_판정_False_점수_nan(self):
        # 0.0은 실제로 나올 수 있는 점수라 "모르는 값"과 구분되어야 한다.
        detector = MLAnomalyDetector(min_samples=100)
        flags, scores = detector.update_batch_with_scores(
            [make_normal_data(seed=i) for i in range(10)]
        )

        assert flags == [False] * 10
        assert all(s != s for s in scores)  # nan

    def test_빈_배치는_빈_결과_두_개(self):
        detector = MLAnomalyDetector(min_samples=10)
        assert detector.update_batch_with_scores([]) == ([], [])


# ── 워밍업 중 재학습 (ADR-018 15차 — 표본 적은 최초 모델이 정상의 91%를 찍었다) ──

class TestWarmupRetrain:

    def test_워밍업_중에는_시간_하한을_무시하고_재학습한다(self):
        # 시간 하한이 걸려 있어도, 윈도우가 안 찬 동안은 표본이 2배가 되면 다시 학습해야
        # 한다 — 안 그러면 200건짜리 모델이 하한 시간만큼 그대로 판정을 내린다.
        detector = MLAnomalyDetector(min_samples=100, window_size=1000,
                                     retrain_min_seconds=3600)
        detector.update_batch([make_normal_data(seed=i) for i in range(100)])
        first = detector.model
        assert detector._trained_with == 100

        detector.update_batch([make_normal_data(seed=100 + i) for i in range(100)])

        assert detector.model is not first
        assert detector._trained_with == 200

    def test_표본이_2배가_되기_전에는_재학습하지_않는다(self):
        detector = MLAnomalyDetector(min_samples=100, window_size=1000,
                                     retrain_min_seconds=3600)
        detector.update_batch([make_normal_data(seed=i) for i in range(100)])
        first = detector.model

        # 100 -> 150. 아직 2배(200)가 아니다.
        detector.update_batch([make_normal_data(seed=100 + i) for i in range(50)])

        assert detector.model is first

    def test_윈도우가_차면_다시_시간_하한이_적용된다(self):
        # 워밍업이 끝난 뒤에도 시간 하한을 무시하면, 원래 고치려던 재학습 폭주로 돌아간다.
        detector = MLAnomalyDetector(min_samples=100, window_size=200,
                                     retrain_interval=50, retrain_min_seconds=3600)
        detector.update_batch([make_normal_data(seed=i) for i in range(200)])
        assert len(detector._buffer) == 200  # 윈도우가 찼다
        filled = detector.model

        detector.update_batch([make_normal_data(seed=300 + i) for i in range(200)])

        assert detector.model is filled  # 시간 하한에 막혀 재학습되지 않는다

    def test_워밍업_재학습_횟수는_log_스케일이다(self):
        # 건수마다 재학습하면 윈도우가 클수록 학습 비용이 선형으로 늘어난다.
        # 2배 조건이면 200 -> 3200 구간에서 다섯 번(400/800/1600/3200 + 최초)이면 된다.
        detector = MLAnomalyDetector(min_samples=200, window_size=3200,
                                     retrain_min_seconds=3600)
        trained_sizes = []
        original_train = detector._train

        def spy():
            original_train()
            trained_sizes.append(detector._trained_with)

        detector._train = spy
        for _ in range(32):
            detector.update_batch([make_normal_data(seed=i) for i in range(100)])

        assert trained_sizes == [200, 400, 800, 1600, 3200]


# ── 점수 임계값 (ADR-018 15차 — contamination은 "표시할 비율"이라 데이터를 안 본다) ──

class TestScoreThreshold:

    def _trained(self, **kw):
        detector = MLAnomalyDetector(min_samples=50, window_size=50,
                                     retrain_min_seconds=3600, **kw)
        detector.update_batch([make_normal_data(seed=i) for i in range(60)])
        return detector

    def test_기본값은_기존_동작을_그대로_유지한다(self):
        # 임계값을 안 주면 모델 offset_로 판정 — 기존 배포의 동작이 바뀌면 안 된다.
        detector = self._trained()
        assert detector.score_threshold is None

        probes = [make_normal_data(seed=200 + i) for i in range(30)]
        flags, scores = detector.update_batch_with_scores(probes)

        offset = detector.model.offset_
        assert flags == [s < offset for s in scores]

    def test_임계값을_주면_그_기준으로_판정한다(self):
        detector = self._trained(score_threshold=-0.55)

        probes = [make_normal_data(seed=200 + i) for i in range(30)]
        flags, scores = detector.update_batch_with_scores(probes)

        assert flags == [s < -0.55 for s in scores]

    def test_임계값이_낮을수록_알림이_줄어든다(self):
        # 점수는 낮을수록 이상이므로, 임계값을 내리면 걸리는 게 줄어야 한다.
        probes = [make_normal_data(seed=200 + i) for i in range(60)]

        loose = self._trained(score_threshold=-0.40)
        tight = self._trained(score_threshold=-0.70)

        assert sum(loose.update_batch_with_scores(probes)[0]) >= \
               sum(tight.update_batch_with_scores(probes)[0])

    def test_단건_경로도_같은_임계값을_쓴다(self):
        # update()가 predict()를 그대로 부르면 임계값을 무시해 두 경로가 갈린다.
        batch_side = self._trained(score_threshold=-0.40)
        single_side = self._trained(score_threshold=-0.40)

        probe = make_normal_data(seed=777)

        assert single_side.update(probe) == batch_side.update_batch([probe])[0]

    def test_아주_높은_임계값이면_전부_이상으로_본다(self):
        detector = self._trained(score_threshold=1.0)  # 점수는 항상 1보다 작다
        probes = [make_normal_data(seed=300 + i) for i in range(20)]

        assert all(detector.update_batch(probes))
