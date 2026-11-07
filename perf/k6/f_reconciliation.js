/**
 * Scenario F: Reconciliation endpoint performance
 * Single-VU or low-VU ops call; measures run duration via http_req_duration.
 *
 *   k6 run -e VUS=1 -e ITERATIONS=5 perf/k6/f_reconciliation.js
 *   k6 run -e LOOKBACK_HOURS=24 -e STUCK_MINUTES=30 perf/k6/f_reconciliation.js
 */
import { check, sleep } from "k6";
import { runReconciliation } from "./helpers.js";

export const options = {
  vus: Number(__ENV.VUS || 1),
  iterations: Number(__ENV.ITERATIONS || 5),
};

export default function () {
  const body = {
    lookbackHours: Number(__ENV.LOOKBACK_HOURS || 24),
    stuckPendingMinutes: Number(__ENV.STUCK_MINUTES || 30),
  };
  if (__ENV.PAYMENT_ID) {
    body.paymentId = __ENV.PAYMENT_ID;
  }
  const res = runReconciliation(body);
  check(res, {
    "status 200": (r) => r.status === 200,
    "has findings array": (r) => {
      try {
        return Array.isArray(JSON.parse(r.body).findings);
      } catch (e) {
        return false;
      }
    },
  });
  sleep(1);
}
