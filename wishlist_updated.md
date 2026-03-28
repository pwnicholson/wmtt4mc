# World Map Timeline Tool for Minecraft (WMTT4MC) — Wishlist

A backlog of ideas to revisit after core stability and correctness are solid.

## Rendering speed & efficiency
- Render multiple frames concurrently (parallelize across backups, not within a single world DB) to better use CPU without hammering a single LevelDB.
- Progressive outputs: generate an animated GIF as soon as 2 frames exist, then update/overwrite the GIF each time a new frame completes. This makes partial results available if a large run is cancelled.

## Crop / camera tools
- Visual crop selection from a quick-scan map preview.
- Auto-crop: pick an area matching the target aspect ratio that maximizes "interesting" chunks (eg, highest density of non-empty chunks).
- Very advanced: auto-pan/auto-zoom through the timelapse to highlight regions of build activity (detect changes between consecutive frames).

## Color, palettes, and visuals
- Make height-based shading more pronounced (hills should read more clearly; keep cliffs/ravines distinct without crushing blacks).
- Biome tint toggle: Off / Basic / Accurate.
  - Off: treat tintable textures as their default (roughly "plains").
  - Basic: apply a single fixed biome tint (plains) everywhere (faster).
  - Accurate: read biome data per column and apply biome-specific grass/foliage/water tint (slower).
- Palette loaded from `palette.json` (instead of being fully hardcoded), so we can update colors without touching core code.
- Optional palette override path in the UI (advanced).
- Flowers rendering toggle (render as full pixel vs treat as transparent) to reduce noise, while still allowing colorful flower fields.

## Higher detail rendering
- Single-map "super resolution" mode (eg, 3x3 pixels per block) to show thin structures (fences, rails, flowers/torches as a small mark) based on block orientation/state.

## Output formats & quality
- File size estimator for frames + final output. Warn if estimated size is large (eg >500MB or >10% of free space on output drive).
- Output naming: default `worldname_wmtt4mc.gif` with user override.
- Warn when crop area contains fewer source pixels than the selected output resolution (quality will be upscaled).
- Optional output video (MP4/WebM) in addition to GIF (advanced).

## UI / usability
- Remember last input/output folders (preferences file).
- Better error handling for missing required inputs (no silent no-ops).

## Known issues to revisit
- Frame order correctness in final GIF when rendering newest→oldest but composing oldest→newest.

## Done / not needed right now
- Cancel process feels good.
- Keep/delete frames option feels good.
