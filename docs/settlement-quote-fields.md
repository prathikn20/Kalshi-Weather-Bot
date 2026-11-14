# Settlement quote-field boundary

This change unblocks the first complete settlement acquisition without changing
live market validation. The task authorizes this contract change only after a
read-only survey finds no label-field failures.

## Read-only survey

The survey used the exact VM backfill within the generation-pinned GCS object
`raw/kalshi_settlements/2026-11-14/2026-11-14T20.tar.zst`, generation
`1794690550894297`, size 3,963,171 bytes, CRC32C `vdGFZA==`, SHA-256
`eae7ddf75bde49a42470afe2517860954013ddf0d7910b75e5432a80fd4c6f12`.
The archive also contains the start of a later daily run; acquisition timestamps
select the 58 backfill envelopes. Each replay source ID and decompressed-envelope
SHA-256 was checked against the archive. No raw record was changed.

Against main at e29064d, all current `SettlementInput` field errors were collected
for every configured record. Label annotations, original rules/bounds logic, payout
consistency and complete-event checks were also evaluated independently of quote
validation, using a quote-free projection of the original annotations. This avoids
letting the known quote failure hide a label failure.

The acquisition contains 47,952 market observations: **30,774 configured records**
and 17,178 legacy/raw-only records. Configured records cover **5,129 events**.
The complete grouped failure table is:

| Field | Failing served value | Observations | Distinct tickers | Example tickers (up to 5) |
|---|---|---:|---:|---|
| yes_bid_size_fp | `"-276.00"` | 1 | 1 | KXHIGHCHI-26MAY13-B60.5 |

**Label-field failures: zero.** Checked result; settlement value, floor and bounds
metadata; settlement timestamp; expiration value; structured/functional/custom
strikes and bounds; primary/secondary rules; and event consistency. There were no
other non-quote failures. The sole permitted event exception remains
KXHIGHMIA-26APR11, stored without labels. The conditional implementation gate passed.

The baseline survey JSON/script and backup/replay verification are retained under
`state/settlement-quotes-2026-11-14/` in the main checkout. The survey is tied to the
original commit because its purpose is to evaluate the pre-change validators.

## Contract and implementation

Kalshi documents prices and quantities as decimal strings in its
[fixed-point guide](https://docs.kalshi.com/getting_started/fixed_point_migration)
and [market schema](https://docs.kalshi.com/api-reference/market/get-markets).
The settlement-only quote boundary checks fixed-point notation: optional leading
minus, ASCII integer digits and an optional decimal fraction. It preserves the
string and performs no sign, magnitude or market-range check. Exponents, whitespace,
nonfinite values, numeric Python values and malformed notation still raise.
Provider-optional quote fields remain optional; required fields remain required.

All 15 recognized quote fields use that boundary: four bid/ask dollar prices,
last price, three previous prices, two YES sizes, volume, 24-hour volume, open
interest, liquidity and notional. Unknown fields still raise. Labels retain all
prior validators and consistency checks.

Stored `Settlement` rows contain none of these 15 quote fields. Shared
`MarketDetails` holds identity, lifecycle, rules and normalized bounds; the existing
market-row function can construct that output for settlements. This avoids passing
terminal quotes through the live snapshot's second validation boundary. Live
`MarketInput` and `MarketSnapshot` retain their exact previous field schemas and
validators, including nonnegative sizes; their JSON schemas were compared with
main and are identical.

Two old tests described superseded behavior. The negative-terminal-size rejection
test now verifies successful parsing, exact label preservation and absent quote
columns. The metadata-revision test uses retained `updated_time` instead of omitted
`volume_fp`; its revision and label-revision assertions remain. Existing live quote
validation was not relaxed. Quote-only changes remain in raw history and are not
stored or used by settlements.

## Full-history parse and publication

The complete acquisition parses successfully: 58 envelopes, one completed run,
30,774 settlement rows, zero errors, zero coverage gaps and zero revisions. No new
event failed consistency. The only exception is KXHIGHMIA-26APR11.

| Configured series | Rows | Events | Labeled rows | Exception rows |
|---|---:|---:|---:|---:|
| KXHIGHAUS | 4,506 | 751 | 4,506 | 0 |
| KXHIGHCHI | 4,506 | 751 | 4,506 | 0 |
| KXHIGHDEN | 4,344 | 724 | 4,344 | 0 |
| KXHIGHLAX | 4,068 | 678 | 4,068 | 0 |
| KXHIGHMIA | 4,506 | 751 | 4,500 | 6 |
| KXHIGHNY | 4,500 | 750 | 4,500 | 0 |
| KXHIGHPHIL | 4,344 | 724 | 4,344 | 0 |

Totals: **30,768 labeled rows and 6 exception rows**. Labels are 5,128 YES,
25,640 NO and 6 absent. The six exception rows preserve served payouts/results.

Run on the Mac from the repository root:

```sh
uv run --locked python -m kwb.parse.settlements   state/settlements-2026-11-14/vm-backfill-replay/kalshi_settlements
```

The canonical store is `data/kalshi/`. Before merging, the full acquisition was
validated and published in an isolated worktree store; canonical publication follows
merge so the main checkout's reader and persisted schema change together.

Point-in-time check through `Store.get_settlements("KXHIGHLAX", as_of)`:

| UTC as_of | Returned rows | Meaning |
|---|---:|---|
| 2026-11-14T20:48:28.079103Z | 0 | One microsecond before the first LAX receipt |
| 2026-11-14T20:48:28.079104Z | 366 | Visible exactly at that envelope's receipt |
| 2026-11-14T20:53:27.956213Z | 4,068 | After the complete backfill |

Every returned row's availability was checked against its cutoff. Availability
remains the original envelope receipt time; parsing/publishing does not reset it.

## Required-property proofs

Each production mutation ran alone, failed behaviorally, and was reverted in
`finally`. Full transcripts are in `state/settlements/proofs/` during development
and retained in the task report directory after cleanup. The runner is
`uv run --locked python scripts/prove_settlements.py`; it also reruns the 20
existing settlement invariant mutations.

### Property 1

`test_a_real_negative_settled_quote_size_parses_with_every_label_field_intact`

Parses the real negative-size event, preserves every retained label field and stores no quote columns.

Mutation: Reinstate the nonnegative size type in SettlementInput.
Observed: **1 failed in 0.20s**, exit 1; source restored.

```text
E                 Value error, Expected a nonnegative fixed-point string [type=value_error, input_value='-276.00', input_type=str]
E                   For further information visit https://errors.pydantic.dev/2.14/v/value_error
```

### Property 2

`test_the_same_negative_quote_size_in_a_live_snapshot_still_raises_with_its_locator`

Rejects the same -276.00 size in a real live-market envelope and retains its locator.

Mutation: Allow signed values in the shared live fixed-point validator.
Observed: **1 failed in 0.25s**, exit 1; source restored.

```text
E       Failed: DID NOT RAISE ValueError
```

### Property 3

`test_a_malformed_fixed_point_quote_in_any_settlement_quote_field_raises`

Rejects exponent notation in every one of the 15 recognized settlement quote fields.

Mutation: Disable the settlement quote-notation guard.
Observed: **15 failed in 0.33s**, exit 1; source restored.

```text
E       Failed: DID NOT RAISE ValueError
E       Failed: DID NOT RAISE ValueError
```

### Property 4

`test_a_settlement_label_field_violation_still_raises`

Rejects malformed, out-of-range or inconsistent label fields with the envelope locator.

Mutation: Disable the label dollar upper-bound check; an invalid settlement floor is then accepted.
Observed: **1 failed, 20 passed in 0.26s**, exit 1; source restored.

```text
E       Failed: DID NOT RAISE ValueError
```

All **24 mutants** failed and were restored. Full restored-code suite:
**470 passed in 13.12 s**. Locked sync, Ruff and format checks are completed before
commit; final publication and cleanup evidence are kept in the task report.

No VM deployment is needed: neither collector nor service units changed. The VM
collector imports only collector helpers and the standard library, while both
modified parsers and the schema boundary are used for Mac parsing. No parser is
scheduled by any deployed unit.
