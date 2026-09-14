import csv, glob, sys, os, collections
E = sys.argv[1]
T = {}
for l in open(os.path.join(E, 'timeline.txt')):
    k, _, v = l.partition(':')
    try: T[k.strip()] = float(v)
    except ValueError: pass
S = T['run_start_ms']
files = sorted(glob.glob(os.path.join(E, 'gen_*.csv'))) + [os.path.join(E, 'live.csv')]
buckets = collections.defaultdict(list)
for p in files:
    rows = list(csv.DictReader(open(p)))
    first = min(float(r['sent_ms']) for r in rows)
    lags = [float(r['sent_ms']) - float(r['intended_ms']) for r in rows]
    big = [(float(r['intended_ms']) - S) / 1000 for r in rows if float(r['sent_ms']) - float(r['intended_ms']) > 1000]
    print(f"{os.path.basename(p)} n={len(rows)} first_sent={(first - S)/1000:+.1f}s lag>1s={len(big)} at_s={[round(x) for x in big[:6]]}...{[round(x) for x in big[-3:]]}")
    for r in rows:
        b = int((float(r['intended_ms']) - S) // 20000) * 20
        buckets[b].append(float(r['sent_ms']) - float(r['intended_ms']))
print("20s bucket (from run start): n, lag_max_ms, lag>1s count")
for b in sorted(buckets):
    v = buckets[b]
    print(f"  {b:4d}s n={len(v)} max={max(v):.0f} over1s={sum(1 for x in v if x > 1000)}")
