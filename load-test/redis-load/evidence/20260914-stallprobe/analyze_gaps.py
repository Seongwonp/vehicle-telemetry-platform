import csv, os, sys
E = sys.argv[1]
T = {}
for l in open(os.path.join(E, 'timeline.txt')):
    k, _, v = l.partition(':')
    try: T[k.strip()] = float(v)
    except ValueError: pass
S = T['run_start_ms']
def gaps(ts, label, thr):
    ts = sorted(set(ts))
    out = [((a - S) / 1000, (b - a) / 1000) for a, b in zip(ts, ts[1:]) if b - a > thr]
    print(f"{label}: 표본 {len(ts)}, 간격>{thr/1000:.0f}s {len(out)}건 ->", [f"{s:+.0f}s:{g:.1f}s" for s, g in out])
m = [int(r['ts_ms']) for r in csv.DictReader(open(os.path.join(E, 'metrics.csv')))]
gaps(m, "sampler(컨테이너, 2s 주기)", 4000)
d = []
for l in open(os.path.join(E, 'dockerstats.csv')):
    p = l.split(',')
    if p[0].isdigit(): d.append(int(p[0]))
gaps(d, "docker stats(호스트 루프)", 8000)
slow = []
for p in ['gen_1.csv','gen_2.csv','gen_3.csv','gen_4.csv','live.csv']:
    for r in csv.DictReader(open(os.path.join(E, p))):
        lat = float(r['end_ms']) - float(r['sent_ms'])
        if lat > 6000:
            slow.append(((float(r['sent_ms']) - S) / 1000, r['path'], r['http'], round(lat)))
print("응답 6s 초과(전송 시각, 경로, http, ms):", sorted(slow))
print("stop_done", (T['stop_done_ms'] - S) / 1000, "start_cmd", (T['start_cmd_ms'] - S) / 1000)
