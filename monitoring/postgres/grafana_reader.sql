-- Grafana 전용 읽기 전용 역할. 대시보드 변수가 "등록된 활성 차량" 목록을 읽는 데만 쓴다.
-- 백엔드 스키마(Flyway)는 바꾸지 않는다 — 운영 역할이라 마이그레이션 밖에서 관리한다.
-- 멱등: 몇 번 돌려도 같은 상태가 된다(역할 생성은 없을 때만, 비밀번호·권한은 매번 맞춘다).
-- 실행: scripts/grafana-pg-reader.sh (비밀번호는 GRAFANA_PG_READER_PASSWORD 환경변수로만 받는다).
-- 되돌리기: 이 파일 끝의 주석 참고.
\set ON_ERROR_STOP on
\getenv pw GRAFANA_PG_READER_PASSWORD

SELECT 'CREATE ROLE grafana_reader LOGIN'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_reader') \gexec

ALTER ROLE grafana_reader WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT
  NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 5 PASSWORD :'pw';
-- 권한으로 이미 막혀 있지만, 쓰기 시도는 세션 수준에서도 거부되게 한다.
ALTER ROLE grafana_reader SET default_transaction_read_only = on;

SELECT format('GRANT CONNECT ON DATABASE %I TO grafana_reader', current_database()) \gexec
GRANT USAGE ON SCHEMA public TO grafana_reader;

-- 표 단위 권한은 하나도 두지 않고, vehicles의 두 열만 연다(owner_id·name 등은 못 읽는다).
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM grafana_reader;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM grafana_reader;
GRANT SELECT (vehicle_id, active) ON vehicles TO grafana_reader;

-- 되돌리기(수동, 데이터 영향 없음):
--   REVOKE ALL ON vehicles FROM grafana_reader;
--   REVOKE SELECT (vehicle_id, active) ON vehicles FROM grafana_reader;
--   REVOKE USAGE ON SCHEMA public FROM grafana_reader;
--   REVOKE CONNECT ON DATABASE <db> FROM grafana_reader;
--   DROP ROLE grafana_reader;
