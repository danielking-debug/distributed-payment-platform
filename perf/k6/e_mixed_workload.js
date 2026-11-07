/**
 * Scenario E: Mixed success / failure workload
 * FAIL_RATIO (0..1) portion of requests use an amount larger than seed balances
 * to drive business FAILED after processing (still 202 on submit).
 *
 *   k6 run -e VUS=10 -e FAIL_RATIO=0.2 -e DURATION=60s perf/k6/e_mixed_workload.js
 *
 * Note: submit always accepts PENDING; FAILED appears only after async processing
 * when Kafka + processor are running and amount exceeds balance.
 */
import { sleep } from "k6";
import { ACCOUNTS } from "./accounts.js";
import { submitPayment, checkAccepted, uuid } from "./helpers.js";

const FAIL_RATIO = Number(__ENV.FAIL_RATIO || 0.2);

export const options = {
  vus: Number(__ENV.VUS || 5),
  duration: __ENV.DURATION || "60s",
};

export default function () {
  const i = Math.floor(Math.random() * ACCOUNTS.length);
  const j = (i + 1) % ACCOUNTS.length;
  const fail = Math.random() < FAIL_RATIO;
  const amount = fail ? "99999999.00" : "1.00";
  const res = submitPayment(ACCOUNTS[i], ACCOUNTS[j], amount, uuid());
  checkAccepted(res);
  sleep(Number(__ENV.THINK || 0.05));
}
