"""STOMP-over-WebSocket 1프레임 수신 프로브 (2026-09-29 trace 검증용).

- 자격증명은 백엔드 .env의 ADMIN_USERNAME/ADMIN_PASSWORD를 읽어 쓴다. 출력·기록하지 않는다.
- JWT도 기록하지 않는다. 저장하는 것은 CONNECTED 프레임(토큰 없음)과 MESSAGE 프레임 본문뿐이다.
사용: python ws_probe.py <vehicleId> <out_dir>
"""
import asyncio, json, sys, time, datetime, pathlib, urllib.request
import websockets

vid, out = sys.argv[1], pathlib.Path(sys.argv[2])
env = {}
for line in pathlib.Path("D:/vehicle-telemetry-platform/.env").read_text(encoding="utf-8").splitlines():
    if "=" in line and not line.lstrip().startswith("#"):
        k, v = line.split("=", 1); env[k.strip()] = v.strip()

def now(): return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")

req = urllib.request.Request("http://localhost:8080/api/auth/login",
    data=json.dumps({"username": env["ADMIN_USERNAME"], "password": env["ADMIN_PASSWORD"]}).encode(),
    headers={"Content-Type": "application/json"})
with urllib.request.urlopen(req, timeout=10) as r:
    status = r.status; token = json.loads(r.read())["accessToken"]
log = [f"{now()} login HTTP {status} (token 미기록)"]

def frame(cmd, headers, body=""):
    return cmd + "\n" + "".join(f"{k}:{v}\n" for k, v in headers.items()) + "\n" + body + "\x00"

async def main():
    async with websockets.connect("ws://localhost:8080/ws", subprotocols=["v12.stomp"], open_timeout=10) as ws:
        log.append(f"{now()} websocket open")
        await ws.send(frame("CONNECT", {"accept-version": "1.2", "host": "localhost",
                                        "heart-beat": "0,0", "Authorization": "Bearer " + token}))
        c = await asyncio.wait_for(ws.recv(), 10)
        log.append(f"{now()} recv CONNECTED: {c.splitlines()[0]} | " + " ".join(c.splitlines()[1:4]))
        (out / "02_connected_frame.txt").write_text(c.replace("\x00", ""), encoding="utf-8")
        await ws.send(frame("SUBSCRIBE", {"id": "sub-0", "destination": f"/topic/vehicle/{vid}/telemetry"}))
        log.append(f"{now()} sent SUBSCRIBE /topic/vehicle/{vid}/telemetry")
        m = await asyncio.wait_for(ws.recv(), 40)
        recv_at = now()
        (out / "03_message_frame.txt").write_text(m.replace("\x00", ""), encoding="utf-8")
        log.append(f"{recv_at} recv first frame: {m.splitlines()[0]}")
        body = m.split("\n\n", 1)[1].rstrip("\x00")
        d = json.loads(body)
        (out / "04_message_body.json").write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
        log.append(f"received vehicleId={d.get('vehicleId')} timestamp={d.get('timestamp')} speed={d.get('speed')}")
        await ws.send(frame("DISCONNECT", {}))

try:
    asyncio.run(main())
except Exception as e:
    log.append(f"{now()} FAILED {type(e).__name__}: {str(e)[:200]}")
(out / "01_probe_log.txt").write_text("\n".join(log) + "\n", encoding="utf-8")
print("\n".join(log))
