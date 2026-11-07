/**
 * Scenario C: Hot-account contention vs distributed pairs
 *
 * MODE:
 *   disjoint  - each VU uses a unique from-account (minimal shared locks)
 *   distributed - random pairs (baseline mix)
 *   same_source - all VUs debit the same source account (lock contention)
 *   same_pair - all VUs use the same from+to pair (maximum contention)
 *
 *   k6 run -e MODE=same_source -e VUS=10 -e DURATION=30s perf/k6/c_hot_account.js
 *   k6 run -e MODE=disjoint -e VUS=10 -e DURATION=30s perf/k6/c_hot_account.js
 */
import { sleep } from "k6";
import { ACCOUNTS } from "./accounts.js";
import { submitPayment, checkAccepted, uuid } from "./helpers.js";

const MODE = __ENV.MODE || "distributed";

export const options = {
  vus: Number(__ENV.VUS || 5),
  duration: __ENV.DURATION || "30s",
};

export default function () {
  let from;
  let to;
  switch (MODE) {
    case "disjoint":
      from = ACCOUNTS[(__VU - 1) % ACCOUNTS.length];
      to = ACCOUNTS[(__VU + 4) % ACCOUNTS.length];
      break;
    case "same_source":
      from = ACCOUNTS[0];
      to = ACCOUNTS[1 + ((__VU - 1) % (ACCOUNTS.length - 1))];
      break;
    case "same_pair":
      from = ACCOUNTS[0];
      to = ACCOUNTS[1];
      break;
    case "distributed":
    default:
      from = ACCOUNTS[Math.floor(Math.random() * ACCOUNTS.length)];
      to = ACCOUNTS[Math.floor(Math.random() * ACCOUNTS.length)];
      break;
  }
  if (from === to) {
    to = ACCOUNTS[(ACCOUNTS.indexOf(from) + 1) % ACCOUNTS.length];
  }
  const res = submitPayment(from, to, "1.00", uuid());
  checkAccepted(res);
  sleep(Number(__ENV.THINK || 0.05));
}
