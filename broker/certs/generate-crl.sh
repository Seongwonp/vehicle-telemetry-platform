#!/bin/bash
# ================================================================
# MQTT 인증서 폐기 목록(CRL) 생성 — broker/certs/crl.pem
#
# 입력: revoked-certs.txt(추적됨, 공개 정보만) + ca.crt + ca.key
# 출력: crl.pem(공개 정보, .gitignore의 *.pem — 다른 인증서 산출물과 같게 추적하지 않는다)
#
# - generate-certs.sh가 끝에서 부른다. CA를 새로 만든 환경(CI 등)에서는 목록의 CA 지문이
#   달라 **빈 CRL**이 나온다. mosquitto.conf의 crlfile이 항상 읽을 파일이 있게 하려는 것이다.
# - 상태를 남기지 않는다. openssl ca용 index.txt·crlnumber는 매번 저장소 밖 임시 디렉터리에
#   목록에서 다시 만들고 지운다. ca.key는 이 호스트 작업에서만 읽고 어떤 컨테이너에도 안 간다.
# - **CRL에도 만료(nextUpdate)가 있다.** 지나면 mosquitto(OpenSSL CRL 검사)가 **정상 차량까지
#   전부** 거부한다. 기본 365일. 만료 전에 이 스크립트를 다시 돌리고
#   `docker compose restart mosquitto`(브로커는 기동할 때만 CRL을 읽는다).
#   남은 기간: openssl crl -in crl.pem -noout -nextupdate
#
# 폐기 추가: 인증서 serial을 확인(openssl x509 -in vehicles/<ID>.crt -noout -serial)해
#   revoked-certs.txt에 한 줄 넣고 이 스크립트 → restart mosquitto.
#
# 사용: bash broker/certs/generate-crl.sh   (CRL_DAYS=365 기본)
# ================================================================
set -euo pipefail

CRL_DAYS="${CRL_DAYS:-365}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

[ -d crl.pem ] && rmdir crl.pem   # 인증서 전에 compose up이 남긴 빈 디렉터리(generate-certs.sh와 같은 가드)

for f in ca.crt ca.key revoked-certs.txt; do
  [ -f "$f" ] || { echo "generate-crl: $f 없음" >&2; exit 1; }
done

ca_fp="$(openssl x509 -in ca.crt -noout -fingerprint -sha256 | sed 's/.*=//; s/://g')"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# openssl ca가 요구하는 최소 DB. 폐기 줄만 있으면 된다(만료 칸은 gencrl이 쓰지 않아 고정값).
: > "$work/index.txt"
echo "unique_subject = no" > "$work/index.txt.attr"
# CRL 번호는 새로 만들 때마다 커져야 한다(받는 쪽이 더 새 CRL을 고른다) — 상태가 없으니 시각으로.
printf '%X\n' "$(date +%s)" > "$work/crlnumber"

count=0
while read -r fp serial revoked_at reason cn _; do
  [ -z "${fp:-}" ] && continue
  case "$fp" in \#*) continue ;; esac
  [ "$fp" = "$ca_fp" ] || continue
  # index.txt의 시각은 UTCTime(YYMMDDHHMMSSZ)
  printf 'R\t491231235959Z\t%s,%s\t%s\tunknown\t/CN=%s\n' \
    "${revoked_at:2}" "$reason" "$serial" "$cn" >> "$work/index.txt"
  count=$((count + 1))
done < <(tr -d '\r' < revoked-certs.txt)

# 상대 경로만 쓴다 — Windows(Git Bash)의 mingw openssl은 설정 파일 안의 /tmp 경로를 못 읽는다.
cat > "$work/openssl.cnf" <<'EOF'
[ ca ]
default_ca = crl_ca
[ crl_ca ]
database = index.txt
crlnumber = crlnumber
default_md = sha256
crl_extensions = crl_ext
[ crl_ext ]
authorityKeyIdentifier = keyid:always
EOF

# 같은 이유로 CA 경로는 Windows에서 cygpath -m(C:/...)으로 넘긴다 — MSYS_NO_PATHCONV=1(generate-certs.sh
# 권장 실행법)이면 Git Bash가 /c/... 경로를 바꿔주지 않는다. Linux(CI)에는 cygpath가 없어 그대로 쓴다.
native_path() { if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s' "$1"; fi; }
if ! (
  cd "$work"
  openssl ca -gencrl -config openssl.cnf \
    -cert "$(native_path "$SCRIPT_DIR/ca.crt")" -keyfile "$(native_path "$SCRIPT_DIR/ca.key")" \
    -crldays "$CRL_DAYS" -out crl.pem 2> openssl-ca.err
); then
  echo "generate-crl: openssl ca -gencrl 실패" >&2
  cat "$work/openssl-ca.err" >&2   # openssl ca의 진단(설정·경로 오류). 키 내용은 나오지 않는다
  exit 1
fi

# 서명 확인 뒤에만 교체한다.
openssl crl -in "$(native_path "$work/crl.pem")" -CAfile ca.crt -noout 2>/dev/null \
  || { echo "generate-crl: CRL 서명 검증 실패" >&2; exit 1; }
cp "$work/crl.pem" crl.pem
chmod 644 crl.pem

echo "      crl.pem 생성: 폐기 ${count}건, $(openssl crl -in crl.pem -noout -nextupdate)"
