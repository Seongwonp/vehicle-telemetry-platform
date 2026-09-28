-- 사용자 테이블과 차량 소유권 FK (ADR-027).
-- 지금까지 사용자는 InMemory admin 한 명, vehicles.owner는 자유 문자열이라 소유권 검사가
-- 실제로는 죽은 경로였다. owner를 users FK로 바꿔 "존재하지 않는 사람 소유" 상태를 스키마가 막는다.

CREATE TABLE IF NOT EXISTS users (
    id            BIGSERIAL PRIMARY KEY,
    -- 100인 이유: 옛 vehicles.owner가 VARCHAR(100)이라 백필이 그 길이를 그대로 받아야 한다.
    -- 새 계정의 username 규칙(3~50자)은 API 검증(UserCreateRequest)이 건다.
    username      VARCHAR(100) NOT NULL UNIQUE,
    -- NULL = 아직 비밀번호가 없어 로그인 불가(백필된 소유자, 또는 기동 시 env로 채워지는 admin).
    password_hash VARCHAR(100),
    role          VARCHAR(10)  NOT NULL CHECK (role IN ('ADMIN', 'USER')),
    active        BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 관리자 계정. 비밀번호 해시는 SQL로 만들 수 없어 기동 시 AdminBootstrap이 ADMIN_PASSWORD로 채운다.
INSERT INTO users (username, role, active)
VALUES ('${admin_username}', 'ADMIN', TRUE)
ON CONFLICT (username) DO UPDATE SET role = 'ADMIN', active = TRUE;

-- 기존 vehicles.owner 문자열을 사용자로 백필한다. 로그인 수단이 없으므로 비활성으로 만든다 —
-- 데이터를 admin 소유로 조용히 바꾸지 않기 위해서다(누가 소유했는지는 그대로 남긴다).
INSERT INTO users (username, role, active)
SELECT DISTINCT owner, 'USER', FALSE
FROM vehicles
WHERE owner IS NOT NULL AND owner <> ''
ON CONFLICT (username) DO NOTHING;

ALTER TABLE vehicles ADD COLUMN IF NOT EXISTS owner_id BIGINT;

UPDATE vehicles v
SET owner_id = u.id
FROM users u
WHERE v.owner_id IS NULL AND u.username = v.owner;

-- owner가 비어 있던 차량은 관리자 소유로 둔다. 이 마이그레이션 이전에 그런 행은 소유자 검사에서
-- 아무도 통과 못 하는 상태였고, 관리자는 어차피 전부 접근할 수 있으므로 권한이 늘어나지 않는다.
UPDATE vehicles
SET owner_id = (SELECT id FROM users WHERE username = '${admin_username}')
WHERE owner_id IS NULL;

ALTER TABLE vehicles ALTER COLUMN owner_id SET NOT NULL;
ALTER TABLE vehicles
    ADD CONSTRAINT fk_vehicles_owner FOREIGN KEY (owner_id) REFERENCES users (id);
ALTER TABLE vehicles DROP COLUMN owner;

-- 목록 조회는 "내 활성 차량"이다(VehicleRepository.findAllByOwner_UsernameAndActiveTrue).
CREATE INDEX IF NOT EXISTS idx_vehicles_owner_active ON vehicles (owner_id) WHERE active;
