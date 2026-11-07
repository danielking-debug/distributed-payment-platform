/**
 * Scenario B: Normal concurrent processing load (disjoint-ish account pairs)
 * Same submit path as A; use higher VUS. Measure submit latency; processing
 * throughput requires Kafka + observing DB/status separately.
 *
 *   k6 run -e VUS=10 -e DURATION=60s perf/k6/b_normal_concurrent.js
 */
import { sleep } from "k6";
import { ACCOUNTS } from "./accounts.js";
import { submitPayment, checkAccepted, uuid } from "./helpers.js";

export const options = {
  stages: [
    { duration: "10s", target: Number(__ENV.VUS || 5) },
    { duration: __ENV.DURATION || "60s", target: Number(__ENV.VUS || 5) },
    { duration: "10s", target: 0 },
  ],
};

export default function () {
  // Pair VU-stable offsets to reduce accidental hot pairing
  const base = (__VU - 1) % ACCOUNTS.length;
  const from = ACCOUNTS[base];
  const to = ACCOUNTS[(base + 3) % ACCOUNTS.length];
  if (from === to) return;
  const res = submitPayment(from, to, "1.00", uuid());
  checkAccepted(res);
  sleep(Number(__ENV.THINK || 0.05));
}
