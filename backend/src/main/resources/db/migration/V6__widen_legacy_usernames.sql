-- V4 수정 전에 이미 적용된 DB도 기존 owner의 최대 길이(100자)에 맞춘다.
-- V4 checksum 전환 절차: docs/runbook/user-schema-upgrade.md
ALTER TABLE users ALTER COLUMN username TYPE VARCHAR(100);
