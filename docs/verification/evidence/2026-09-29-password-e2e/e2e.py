# 비밀번호 변경·초기화 E2E. 관리자 자격증명은 .env에서 읽되 출력하지 않는다.
# 테스트 사용자 비밀번호는 실행마다 무작위로 만들고 메모리에만 둔다. 토큰·비밀번호는 기록하지 않고 상태 코드와 code만 기록한다.
import json, secrets, sys, time, urllib.request, urllib.error
BASE = "http://localhost:8080"
env = {}
for line in open(".env", encoding="utf-8"):
    if "=" in line and not line.startswith("#"):
        k, v = line.rstrip("\n").split("=", 1); env[k] = v
ADMIN, APW = env["ADMIN_USERNAME"], env["ADMIN_PASSWORD"]
user = "e2e-pw-" + time.strftime("%m%d%H%M%S")
pw1, pw2, pw3 = ("Aa1-" + secrets.token_urlsafe(12) for _ in range(3))
log = open(sys.argv[1], "w", encoding="utf-8")

def call(step, method, path, body=None, token=None):
    data = json.dumps(body).encode() if body is not None else None
    h = {"Content-Type": "application/json"}
    if token: h["Authorization"] = "Bearer " + token
    req = urllib.request.Request(BASE + path, data=data, method=method, headers=h)
    t0 = time.time()
    try:
        r = urllib.request.urlopen(req, timeout=15); code, raw = r.status, r.read()
    except urllib.error.HTTPError as e:
        code, raw = e.code, e.read()
    try: js = json.loads(raw) if raw else {}
    except Exception: js = {}
    err = js.get("code") if isinstance(js, dict) else None
    line = f"{time.strftime('%H:%M:%SZ', time.gmtime())} {step:<44} {method} {path.replace(ADMIN, "{admin}"):<32} -> {code}" + (f" code={err}" if err else "") + f" ({int((time.time()-t0)*1000)}ms)"
    print(line); log.write(line + "\n"); log.flush()
    return code, js

_, a = call("1 관리자 로그인", "POST", "/api/auth/login", {"username": ADMIN, "password": APW})
at = a["accessToken"]
call(f"2 사용자 생성 {user}", "POST", "/api/users", {"username": user, "password": pw1, "role": "USER"}, at)
_, u = call("3 사용자 로그인", "POST", "/api/auth/login", {"username": user, "password": pw1})
uat, urt = u["accessToken"], u["refreshToken"]
call("4 현재 비밀번호 틀림", "POST", "/api/auth/password", {"currentPassword": "wrong-" + pw1, "newPassword": pw2}, uat)
call("4b 새 비밀번호 정책 위반(7자)", "POST", "/api/auth/password", {"currentPassword": pw1, "newPassword": "Aa1-xyz"}, uat)
call("5 현재 비밀번호 맞음 → 변경", "POST", "/api/auth/password", {"currentPassword": pw1, "newPassword": pw2}, uat)
call("6 옛 refresh로 재발급", "POST", "/api/auth/refresh", {"refreshToken": urt})
call("7 옛 비밀번호로 로그인", "POST", "/api/auth/login", {"username": user, "password": pw1})
_, u2 = call("8 새 비밀번호로 로그인", "POST", "/api/auth/login", {"username": user, "password": pw2})
call("9 일반 사용자가 관리자 비밀번호 초기화", "PUT", f"/api/users/{ADMIN}/password", {"newPassword": pw3}, u2["accessToken"])
call("9b 관리자가 없는 사용자 초기화", "PUT", "/api/users/e2e-pw-nonexistent/password", {"newPassword": pw3}, at)
call("10 관리자가 사용자 초기화", "PUT", f"/api/users/{user}/password", {"newPassword": pw3}, at)
call("11 초기화 전 발급된 refresh로 재발급", "POST", "/api/auth/refresh", {"refreshToken": u2["refreshToken"]})
call("12 초기화 비밀번호로 로그인", "POST", "/api/auth/login", {"username": user, "password": pw3})
call("12b 변경 전 비밀번호(pw2)로 로그인", "POST", "/api/auth/login", {"username": user, "password": pw2})
log.write(f"test_user={user}\n")
