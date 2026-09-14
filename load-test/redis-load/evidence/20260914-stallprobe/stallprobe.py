import sys, time
dur, out = float(sys.argv[1]), sys.argv[2]
end = time.time() + dur
with open(out, 'w') as f:
    f.write("t_ms,overshoot_ms\n")
    while time.time() < end:
        t0 = time.time(); time.sleep(0.1); d = (time.time() - t0) * 1000 - 100
        if d > 200:
            f.write(f"{int(t0*1000)},{d:.0f}\n"); f.flush()
