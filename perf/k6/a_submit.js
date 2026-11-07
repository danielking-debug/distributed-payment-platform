/**
 * Scenario A: Payment submission throughput
 * Unique Idempotency-Key each request; rotates across seed accounts.
 *
 *   k6 run -e BASE_URL=http://localhost:8080 -e VUS=1 -e DURATION=30s perf/k6/a_submit.js
 */
import { sleep } from "k6";
import { ACCOUNTS } from "./accounts.js";
import { submitPayment, checkAccepted, uuid } from "./helpers.js";

export const options = {
  vus: Number(__ENV.VUS || 1),
  duration: __ENV.DURATION || "30s",
  thresholds: {
    // Thresholds are soft documentation; adjust after baselines exist.
    http_req_failed: ["rate<0.05"],
  },
};

export default function () {
  const i = Math.floor(Math.random() * ACCOUNTS.length);
  const j = (i + 1 + Math.floor(Math.random() * (ACCOUNTS.length - 1))) % ACCOUNTS.length;
  const res = submitPayment(ACCOUNTS[i], ACCOUNTS[j], __ENV.AMOUNT || "1.00", uuid());
  checkAccepted(res);
  sleep(Number(__ENV.THINK || 0.1));
}
