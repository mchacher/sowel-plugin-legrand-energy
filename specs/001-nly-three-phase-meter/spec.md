# Spec 001 — NLY three-phase meter support

> This repo has no pre-existing `specs/` convention (unlike Sowel core). This
> folder is created ad hoc, following the same format as core specs, to keep
> the same rigor for this contribution. See companion spec
> `sowel` core repo `specs/132-three-phase-live-breakdown/` for the paired
> UI feature.

## Context

The user (Romain / alpitux) owns a **Legrand Compteur d'énergie triphasé
connecté Drivia with Netatmo (ref. 412175, EAN 3414972324892, 3 tores
Rogowski 125A)**, working correctly in the Netatmo Home + Control app
(consumption visible per phase), but invisible in Sowel through this plugin.

## Goal

Discover and report data from three-phase Legrand/Netatmo energy meters,
without changing OAuth, polling cadence, or the bridge-level energy
accumulation logic that already works for `NLPC` meters.

## Scope

### In scope

- Broaden meter discovery (`discoverMeters()`) to accept module type `NLY` in
  addition to `NLPC`.
- No change to main-meter selection, OAuth, or `getMeasure` energy-window
  polling.

### Out of scope

- Per-phase historical energy (Wh) — the Netatmo API doesn't expose it (see
  companion core spec's "Out of scope").
- Adding a test framework to this repo (see "Testing" below — separate
  decision, not bundled into this fix).

## Root cause

Raw `GET /api/homesdata` (owner's real OAuth credentials) shows the meter as
**4 modules of type `NLY`**, all sharing one bridge (`NLG` "Module Control"):

```
type=NLY  id=...78     name="Total"     bridge=<Module Control>
type=NLY  id=...78#1   name="Phase 1"   bridge=<Module Control>
type=NLY  id=...78#2   name="Phase 2"   bridge=<Module Control>
type=NLY  id=...78#3   name="Phase 3"   bridge=<Module Control>
```

`discoverMeters()` filtered on `mod.type !== "NLPC"`, silently dropping all 4
— no error, no device created. `NLY` modules expose only `power` (W) via
`homestatus`, not `sum_energy_elec` (unlike `NLPC`); the main meter's `energy`
(Wh) still comes from the existing bridge-level `getMeasure` call, unaffected.

## Acceptance criteria

- [x] AC1 — All 4 `NLY` modules are discovered as devices (`Total`,
      `Phase 1/2/3`).
- [x] AC2 — `Total` is selected as the main meter (bridge-level `energy`
      accumulation target), same selection rule as before (first module with
      a bridge).
- [x] AC3 — Each `NLY` device's `power` updates live via `homestatus` polling.
- [x] AC4 — No regression for existing `NLPC`-based installs (same filter
      superset, not a replacement).

## Edge cases

| Case                                         | Expected                                  |
| ----------------------------------------------- | -------------------------------------------- |
| Home has only `NLPC` meters (no `NLY`)          | Unchanged behavior (superset filter)          |
| Home has only `NLY` meters (no `NLPC`)          | All 4 discovered, `Total` picked as main       |
| Home has both `NLPC` and `NLY`                  | All discovered; main meter = first w/ bridge  |
| A module type is neither `NLPC` nor `NLY`       | Still ignored (e.g. `NLPO` EV charger)         |

## Testing

This repo has **no existing test framework** (no vitest, no `*.test.ts`,
`package.json` has no `test` script). Adding one is a separate scope decision
from this fix — flagged to the user, not bundled in here. Verification for
this change is: `tsc` clean compile + live verification against the real
meter (see Test Plan below), consistent with how every previous change to
this plugin has been verified.

### Test Plan (manual/live — no automated suite exists)

| Scenario                                                        | Expected                                                     | Verified how                        |
| ------------------------------------------------------------------ | ---------------------------------------------------------------- | -------------------------------------- |
| Unmodified plugin against the real 3-phase meter                    | 0 meters discovered, no error (reproduces the bug)                | Live: `GET /api/v1/devices`, logs      |
| Raw `homesdata`/`homestatus` against the real account                | Confirms `NLY` × 4, `power` field, bridge shared                  | Direct API calls with owner's OAuth token |
| Patched plugin, `tsc` build                                          | Clean compile, zero errors                                        | `tsc` in ephemeral `node:20` container |
| Patched plugin deployed live                                         | 4 devices discovered, `power` live-updating, `Total.energy` accumulating via existing bridge poll | Live: logs + `GET /api/v1/devices`     |
| Existing installed plugins/recipes on the same instance after restart | No regression                                                     | Live: `GET /api/v1/plugins`, `GET /api/v1/recipe-instances` |
