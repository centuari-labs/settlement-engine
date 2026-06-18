# settlement-engine — Dependency Vulnerability Scan (2026-06-08)

Audit-prep phase **0.3** (hub-only external-audit scope). Tool: `pnpm audit` (pnpm 10.29.2).
Raw machine-readable scan: [`dependency-scan-2026-06-08.json`](./dependency-scan-2026-06-08.json)
(captured **pre-remediation** — it is the evidence of what was found; see "Result after remediation" below for current state).

## Summary

| | Pre-remediation | Post-remediation |
|---|---|---|
| Critical | 1 | **0** |
| High | 9 | **0** |
| Moderate | 5 | **0** |
| Low | 2 | **0** |
| **Total** | **17** | **0 — "No known vulnerabilities found"** |

**Key finding:** every Critical/High advisory was in the **dev/test toolchain only**
(`jest`, `ts-jest`, `@types/jest`, `ioredis-mock`) and is never shipped — the production image's
runtime closure (`dotenv`, `ioredis`, `pg`, `pino`, `viem`, `zod`, `@centuari-labs/on-chain-effects`)
contained exactly one advisory: a **moderate** `ws` issue pulled transitively by `viem`. All 17 were
remediated via narrowly-scoped pnpm `overrides`; nothing was left "accepted".

## Critical / High findings (pre-remediation)

| Sev | Package | Resolved | Pulled by | Reachability | Advisory |
|---|---|---|---|---|---|
| critical | handlebars | 4.7.8 | `ts-jest > handlebars` | dev/test only | JS injection via AST type confusion ([GHSA-2w6w-674q-4c4q](https://github.com/advisories/GHSA-2w6w-674q-4c4q)) |
| high | handlebars | 4.7.8 | `ts-jest > handlebars` | dev/test only | 4 × injection / DoS ([3mfm-83xf-c92r](https://github.com/advisories/GHSA-3mfm-83xf-c92r), [xhpv-hc6g-r9c6](https://github.com/advisories/GHSA-xhpv-hc6g-r9c6), [9cx6-37pm-9jff](https://github.com/advisories/GHSA-9cx6-37pm-9jff), [xjpj-3mr7-gcpf](https://github.com/advisories/GHSA-xjpj-3mr7-gcpf)) |
| high | minimatch | 3.1.2 | `jest > @jest/core > @jest/reporters > glob > minimatch` | dev/test only | 3 × ReDoS ([3ppc-4f35-3m26](https://github.com/advisories/GHSA-3ppc-4f35-3m26), [7r86-cg39-jmmj](https://github.com/advisories/GHSA-7r86-cg39-jmmj), [23c5-xmqv-rm74](https://github.com/advisories/GHSA-23c5-xmqv-rm74)) |
| high | picomatch | 2.3.1 | `@types/jest > expect > jest-message-util > micromatch > picomatch` | dev/test only | ReDoS via extglob ([c2c7-rcm5-vvqj](https://github.com/advisories/GHSA-c2c7-rcm5-vvqj)) |
| high | tmp | 0.2.5 | `ioredis-mock > fengari > tmp` | dev/test only | Path traversal via prefix/postfix ([ph9p-34f9-6g65](https://github.com/advisories/GHSA-ph9p-34f9-6g65)) |

## Moderate / Low (pre-remediation)

| Sev | Package | Resolved | Pulled by | Reachability |
|---|---|---|---|---|
| moderate | **ws** | 8.18.3 | **`viem > ws`** | **production runtime** |
| moderate | handlebars | 4.7.8 | `ts-jest` | dev/test only |
| moderate | picomatch | 2.3.1 | `@types/jest > … > micromatch` | dev/test only |
| moderate | brace-expansion | 1.1.12 | `jest > … > minimatch > brace-expansion` | dev/test only |
| low | handlebars | 4.7.8 | `ts-jest` | dev/test only |
| low | diff | 4.0.2 | `jest > … > ts-node > diff` | dev/test only |

## Disposition — remediated, none accepted

Remediated via `pnpm.overrides` in `package.json`, regenerated `pnpm-lock.yaml`, and verified with a
fresh `pnpm audit` ("No known vulnerabilities found"). Each override pins to the **smallest patched
version within the major the consumer expects** — deliberately *not* the latest major — to avoid
breaking the transitive consumer. (Initial attempt with `>=` ranges jumped majors and broke coverage:
`minimatch@10` switched to a named export, so `test-exclude`/`glob`, which `require('minimatch')` as a
function, failed with `minimatch is not a function`. Pinning to `^3.1.4` → 3.1.5 keeps the function
export and is still patched.)

| Override | Before → After | Why this bound |
|---|---|---|
| `ws@<8.20.1` → `^8.20.1` | 8.18.3 → 8.21.0 | viem expects ws 8.x; **only production-reachable fix** |
| `handlebars@<4.7.9` → `^4.7.9` | 4.7.8 → 4.7.9 | ts-jest expects 4.x |
| `minimatch@<3.1.4` → `^3.1.4` | 3.1.2 → 3.1.5 | glob@7 / test-exclude@6 call it as a function → must stay 3.x |
| `picomatch@<2.3.2` → `^2.3.2` | 2.3.1 → 2.3.2 | micromatch@4 expects picomatch 2.x |
| `brace-expansion@<1.1.13` → `^1.1.13` | 1.1.12 → 1.1.15 | minimatch@3.x expects brace-expansion 1.x |
| `tmp@<0.2.6` → `^0.2.6` | 0.2.5 → 0.2.7 | fengari expects tmp 0.2.x |
| `diff@<4.0.4` → `^4.0.4` | 4.0.2 → 4.0.4 | ts-node expects diff 4.x |

**Verification:** full test suite `TZ=UTC pnpm run test` → **314/314 passing** after the bumps; coverage
instrumentation works; `pnpm audit` clean. No production-runtime behavior changed (the only prod-tree
bump, `ws`, is a patch-level move within viem's accepted 8.x range).

## Accepted risks

None. All 17 advisories were remediated.
