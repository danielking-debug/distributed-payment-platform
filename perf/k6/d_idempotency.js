/**
 * Scenario D: Idempotency replay
 * setup() creates one payment; default() repeatedly replays the same key+body.
 * Expect 202 and same payment id; no additional money movement after first process.
 *
 *   k6 run -e VUS=10 -e DURATION=30s perf/k6/d_idempotency.js
 */
import { check, sleep } from "k6";
import { ACCOUNTS } from "./accounts.js";
import { submitPayment, uuid } from "./helpers.js";

export const options = {
  vus: Number(__ENV.VUS || 5),
  duration: __ENV.DURATION || "30s",
};

export function setup() {
  const key = `perf-idem-${uuid()}`;
  const from = ACCOUNTS[0];
  const to = ACCOUNTS[1];
  const res = submitPayment(from, to, "1.00", key);
  let paymentId = null;
  try {
    paymentId = JSON.parse(res.body).id;
  } catch (e) {}
  return { key, from, to, paymentId, status: res.status };
}

export default function (data) {
  const res = submitPayment(data.from, data.to, "1.00", data.key);
  check(res, {
    "replay status 202": (r) => r.status === 202,
    "same payment id": (r) => {
      try {
        return JSON.parse(r.body).id === data.paymentId;
      } catch (e) {
        return false;
      }
    },
  });
  sleep(Number(__ENV.THINK || 0.05));
}
