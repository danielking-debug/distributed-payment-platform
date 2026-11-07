/**
 * Seed account UUIDs from V2__seed_accounts.sql (Load Test Account 01–20).
 * Do not hardcode balances assumptions beyond "funded for small transfers".
 */
export const ACCOUNTS = [
  "a257995f-df03-5e89-9b00-2939712931d3", // 01
  "82b991e9-9951-5e31-a77a-eb665ebb6963", // 02
  "0e3a32fb-4921-5514-8fd3-ade65ccb2924", // 03
  "cbffbf59-ce55-5611-bc80-41bf5a6910b7", // 04
  "19c638ed-d1e1-5941-a5c9-6cbc91754980", // 05
  "f24fd3c6-1966-59fa-b41f-24c17afb87a7", // 06
  "ac65baae-9058-5a39-b157-50497f13d1e3", // 07
  "0c2a3351-223d-53db-b7f3-5485d5e95220", // 08
  "3dac0998-4ac7-5ffa-b213-78ef5a7a1582", // 09
  "32f2617b-b89a-5492-bd15-f1cac49eb122", // 10
  "2f6a5aee-65f8-5c3b-9ae9-512872c83da2", // 11
  "934cc949-4691-599e-af0d-d6bcc5489052", // 12
  "eefc973c-79d8-596a-8ac7-6c3a6bc21b12", // 13
  "2c8df905-6bf2-59ab-a7d1-aec0f917b992", // 14
  "e47a4635-c97a-52f0-b3f5-186988089889", // 15
  "9e907db7-57fc-54a2-9531-8b78de7c5f58", // 16
  "c75c2a61-6c3e-5365-9106-de6bf6775e9a", // 17
  "83b160ab-cb64-53b9-b61e-d012b752388f", // 18
  "c749055b-178f-550c-9779-095a9a1a11b1", // 19
  "e461e79b-4d9a-5d42-b9ca-3aee901cce57", // 20
];

export const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
export const CURRENCY = "USD";
export const AMOUNT = __ENV.AMOUNT || "1.00";
