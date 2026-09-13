"""실험 전용 로컬 webhook 수신기 — Alertmanager가 **발송한 시각**을 남긴다.

외부로 아무것도 보내지 않는다. 받은 요청마다 한 줄 JSON을 stdout에 쓴다:
  {"received_ms": ..., "status": "firing|resolved", "alerts": [{"name", "status", "startsAt", "endsAt"}]}

run_outage.sh가 `docker logs`로 이 줄을 모아 evidence에 남긴다.
annotations·labels 전체는 남기지 않는다 — 알림 이름과 상태·시각만 필요하다.
"""
import json
import time
from http.server import BaseHTTPRequestHandler, HTTPServer


class Sink(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802 (http.server 규약)
        received_ms = int(time.time() * 1000)
        length = int(self.headers.get("Content-Length", "0"))
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            body = {}
        alerts = [
            {
                "name": a.get("labels", {}).get("alertname"),
                "status": a.get("status"),
                "startsAt": a.get("startsAt"),
                "endsAt": a.get("endsAt"),
            }
            for a in body.get("alerts", [])
        ]
        print(json.dumps({"received_ms": received_ms, "status": body.get("status"),
                          "alerts": alerts}, ensure_ascii=False), flush=True)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):  # 접근 로그는 끈다 — 위 한 줄만 남긴다
        pass


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8080), Sink).serve_forever()
