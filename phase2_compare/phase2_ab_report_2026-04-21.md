# Phase 2 A/B Discovery Report (2026-04-21)

## Inputs
- Folder: F:/Development/WMTTT4MC-Master/Season Eight SAMPLE
- Dimension: minecraft:overworld
- Baseline commit: af4cf4e (pre-v1.6.0)
- Current commit: 58159a3

## High-Level Result
- Discovery outputs are identical between baseline and current for this folder/dimension.

## Counts
- Baseline raw sources: 4
- Baseline resolved items: 4
- Baseline resolved as cache: 2
- Baseline resolved as raw: 2
- Current raw sources: 4
- Current resolved items: 4
- Current resolved as cache: 2
- Current resolved as raw: 2

## Resolved Inputs (Current)
- Season Eight 2025-10-11.zip => kind=cache source=cache
- Season Eight 2025-12-11.zip => kind=cache source=cache
- Season Eight 2026-03-04.zip => kind=zip source=raw
- Season Eight 2026-03-27.zip => kind=zip source=raw

## Latest Run Correlation
- Latest run log confirms: cache-backed=2, raw-only=2, total=4.
- Missing expected caches are for 2026-03-27 and 2026-03-04 overworld snapshots.
- This aligns with both baseline and current discovery outputs.

## Conclusion
- For this dataset, regression is not in snapshot discovery selection logic between af4cf4e and current HEAD.
- Primary issue remains operational: missing sidecar caches for newest overworld backups, forcing expensive raw path that times out.
