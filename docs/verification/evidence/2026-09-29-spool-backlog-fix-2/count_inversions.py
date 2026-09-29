"""완료 로그(`[Kafka] 전송 완료`·`spool 드레인 완료`)에서 같은 (차량, 파티션)의 offset 순으로 볼 때
ts가 앞 건보다 줄어드는 인접 쌍 수를 센다. 1차 관찰(2026-09-29-spool-backlog-fix §4, 132쌍)과 같은 정의.

usage: python count_inversions.py <backend_log>
"""
import re
import sys
from collections import defaultdict

LINE = re.compile(
    r"(?P<kind>전송 완료|spool 드레인 완료) — vehicle=(?P<v>\S+) ts=(?P<ts>\S+) "
    r"partition=(?P<p>\d+) offset=(?P<o>\d+)")

rows = defaultdict(list)
kinds = defaultdict(int)
for line in open(sys.argv[1], encoding="utf-8", errors="replace"):
    m = LINE.search(line)
    if not m:
        continue
    kinds[m["kind"]] += 1
    rows[(m["v"], int(m["p"]))].append((int(m["o"]), m["ts"], m["kind"]))

total = 0
by_kind = defaultdict(int)
examples = []
for key, items in sorted(rows.items()):
    items.sort()
    for (o1, t1, k1), (o2, t2, k2) in zip(items, items[1:]):
        if t2 < t1:
            total += 1
            by_kind[("drain" if "spool" in k1 else "direct") + "->" + ("drain" if "spool" in k2 else "direct")] += 1
            if len(examples) < 10:
                examples.append(f"{key[0]} p{key[1]} offset {o1}({'drain' if 'spool' in k1 else 'direct'}) ts={t1} -> offset {o2}({'drain' if 'spool' in k2 else 'direct'}) ts={t2}")

print(f"direct_sent_lines={kinds['전송 완료']} drained_lines={kinds['spool 드레인 완료']}")
print(f"inversion_pairs={total}")
for k, n in sorted(by_kind.items()):
    print(f"  {k}={n}")
for e in examples:
    print(e)
