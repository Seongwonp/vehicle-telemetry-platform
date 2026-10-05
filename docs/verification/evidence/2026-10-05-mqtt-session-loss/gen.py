import json,sys,datetime
vid=sys.argv[1]; n=int(sys.argv[2]); base=datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
for i in range(n):
    ts=(base+datetime.timedelta(milliseconds=i+1)).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]+'Z'
    print(json.dumps({"vehicle_id":vid,"timestamp":ts,"speed":60.5,"rpm":2000.25,"engine_temp":90.0,"throttle_position":20.0,"fuel_level":50.0,"battery_voltage":12.6,"gps":{"lat":37.5,"lng":127.0},"dtc_codes":[]}))
