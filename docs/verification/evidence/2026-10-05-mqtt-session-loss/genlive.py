import json,sys,time,datetime
# usage: genlive.py VID N INTERVAL_S -> emits one payload per interval, timestamp = now (ms), seq in rpm-free field order
vid=sys.argv[1]; n=int(sys.argv[2]); dt=float(sys.argv[3])
for i in range(n):
    ts=datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+'Z'
    print(json.dumps({"vehicle_id":vid,"timestamp":ts,"speed":60.5,"rpm":2000.25,"engine_temp":90.0,"throttle_position":20.0,"fuel_level":50.0,"battery_voltage":12.6,"gps":{"lat":37.5,"lng":127.0},"dtc_codes":[]}),flush=True)
    time.sleep(dt)
