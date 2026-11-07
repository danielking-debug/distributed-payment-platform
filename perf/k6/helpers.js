import { check } from "k6";
import http from "k6/http";
import { BASE_URL, CURRENCY, AMOUNT } from "./accounts.js";

export function uuid() {
  // RFC4122-ish random UUID for Idempotency-Key (not cryptographically strong)
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export function submitPayment(fromAccountId, toAccountId, amount, idempotencyKey) {
  const payload = JSON.stringify({
    fromAccountId: fromAccountId,
    toAccountId: toAccountId,
    amount: amount || AMOUNT,
    currency: CURRENCY,
  });
  const headers = {
    "Content-Type": "application/json",
    "Idempotency-Key": idempotencyKey || uuid(),
  };
  return http.post(`${BASE_URL}/api/v1/payments`, payload, { headers, tags: { name: "POST /payments" } });
}

export function checkAccepted(res) {
  return check(res, {
    "status is 202": (r) => r.status === 202,
    "has payment id": (r) => {
      try {
        return !!JSON.parse(r.body).id;
      } catch (e) {
        return false;
      }
    },
  });
}

export function runReconciliation(body) {
  const payload = body ? JSON.stringify(body) : "{}";
  return http.post(`${BASE_URL}/api/v1/ops/reconciliation/run`, payload, {
    headers: { "Content-Type": "application/json" },
    tags: { name: "POST /ops/reconciliation/run" },
  });
}
