// Track B REST API 부하 테스트 (docs/load-test-plan.md 4절)
//
// 사용법:
//   k6 run -e ENDPOINT=latest    -e BASE_URL=http://localhost:8080 load-test/rest_api.js
//   k6 run -e ENDPOINT=recent    ...
//   k6 run -e ENDPOINT=anomalies ...
//   k6 run -e ENDPOINT=login     ...
//
// 주의: RATE_LIMIT_RPM이 낮으면(기본 60) 성능 측정 전에 .env의 값을 크게 올리고
// backend 서비스를 재생성해야 한다. 측정 후에는 반드시 60으로 원복한다.
// login은 Rate Limit 제외 대상이다.
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const VEHICLE_ID = __ENV.VEHICLE_ID || 'SIM-001';
const ENDPOINT = __ENV.ENDPOINT || 'latest'; // latest | recent | anomalies | login
const USERNAME = __ENV.USERNAME;
const PASSWORD = __ENV.PASSWORD;

export const options = {
  stages: [
    { duration: '30s', target: 50 },  // ramp up
    { duration: '2m', target: 50 },   // steady
    { duration: '30s', target: 200 }, // spike
    { duration: '1m', target: 200 },
    { duration: '30s', target: 0 },   // ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<500'], // 관찰용 목표, 결과는 실측대로 기록
    http_req_failed: ['rate<0.01'],
  },
};

function login() {
  const res = http.post(
    `${BASE}/api/auth/login`,
    JSON.stringify({ username: USERNAME, password: PASSWORD }),
    { headers: { 'Content-Type': 'application/json' } }
  );
  return res.json('accessToken');
}

export function setup() {
  if (!USERNAME || !PASSWORD) {
    throw new Error('USERNAME과 PASSWORD를 환경변수로 주입해야 합니다.');
  }
  if (ENDPOINT === 'login') return {};
  const token = login();
  if (!token) {
    throw new Error('로그인 실패 — setup에서 accessToken을 못 받음. 백엔드/계정 확인 필요.');
  }
  return { token };
}

export default function (data) {
  let res;

  if (ENDPOINT === 'login') {
    res = http.post(
      `${BASE}/api/auth/login`,
      JSON.stringify({ username: USERNAME, password: PASSWORD }),
      { headers: { 'Content-Type': 'application/json' } }
    );
    check(res, { 'login 200': (r) => r.status === 200 });
  } else {
    const params = { headers: { Authorization: `Bearer ${data.token}` } };
    if (ENDPOINT === 'latest') {
      res = http.get(`${BASE}/api/vehicles/${VEHICLE_ID}/telemetry/latest`, params);
    } else if (ENDPOINT === 'recent') {
      res = http.get(`${BASE}/api/vehicles/${VEHICLE_ID}/telemetry?limit=100`, params);
    } else if (ENDPOINT === 'anomalies') {
      res = http.get(`${BASE}/api/vehicles/${VEHICLE_ID}/anomalies?limit=100`, params);
    } else {
      throw new Error(`알 수 없는 ENDPOINT: ${ENDPOINT}`);
    }
    check(res, { 'status 200': (r) => r.status === 200 });
  }

  sleep(1);
}
