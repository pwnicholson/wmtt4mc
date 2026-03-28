# World Map Timeline Tool for Minecraft (WMTT4MC) — Wishlist

A backlog of ideas to revisit after core stability and correctness are solid.

## Rendering speed & efficiency
- Render multiple frames concurrently (parallelize across backups, not within a single world DB) to better use CPU without hammering a single LevelDB.
- Progressive outputs: generate an animated GIF as soon as 2 frames exist, then update/overwrite the GIF each time a new frame completes (so partial results are viewable if a run is cancelled).
- Extract-only-needed world data from backups (avoid unpacking full server archives; only worlds + relevant dimension folders) to reduce disk + time.

## Crop / camera tools
- Visual crop selection from a quick-scan map preview.
- Auto-crop: pick an area matching the target aspect ratio that maximizes "interesting" chunks (eg, highest density of non-empty chunks).
- Frame alignment / no-jump mode:
  - Auto-align frames so the same world coordinates stay registered even as the discovered bounds grow over time (no manual crop required).
  - When a crop box is specified, always render exactly that crop for every frame (even if large areas are blank early on) — currently the image bounds are re-derived from discovered chunks inside the crop box, not pinned to the user's exact coordinates, so jumping can still occur.
- Very advanced: auto-pan/auto-zoom through the timelapse to highlight regions of build activity (detect changes between consecutive frames).

## Color, palettes, and visuals
- Make height-based shading more pronounced / increase contrast (hills should read more clearly; keep cliffs/ravines distinct without crushing blacks).
- Biome tint toggle: Off / Basic / Accurate.
  - Off: treat tintable textures as their default (roughly "plains").
  - Basic: apply a single fixed biome tint (plains) everywhere (faster).
  - Accurate: read biome data per column and apply biome-specific grass/foliage/water tint (slower).
- Optional palette override path in the UI (advanced) — currently palette.json is auto-located next to the script/exe; a UI field would let users point to a specific file.
- Palette editor/viewer tool (GUI): browse/search blocks, edit colors, and mark blocks as transparent/ignored.
- Palette generator tool: sample average colors from a provided resource pack (Java + Bedrock), without distributing the pack.
- Flowers rendering toggle (render as full pixel vs treat as transparent) to reduce noise, while still allowing colorful flower fields.
- "Night mode" (advanced): dim all blocks except within a radius of light sources, accounting for light level falloff.

## Higher detail rendering
- Single-map "super resolution" mode (eg, 3x3 pixels per block) to show thin structures (fences, rails, flowers/torches as a small mark) based on block orientation/state.

## Output formats & quality
- File size estimator for frames + final output. Warn if estimated size is large (eg >500MB or >10% of free space on output drive).
- Warn when crop area contains fewer source pixels than the selected output resolution (quality will be upscaled); optionally suggest super-resolution when available.
- Optional output video (MP4/H.264, WebM) in addition to GIF (advanced). Likely requires user-provided `ffmpeg` path; explore bundling alternatives if feasible.

## UI / usability
- Improve advanced sections consistency (both tabs): advanced controls + log viewer hidden by default.

## Done
- **Palette loaded from `palette.json`** — implemented; app writes a starter `palette.json` next to itself on first run and loads overrides from it at startup.
- **Remember last input/output folders** — implemented via persistent per-user config file.
- **Output naming: `worldname_wmtt4mc.gif` with user override** — implemented.
- **Better error handling for missing required inputs** — implemented via input validation before render starts.
- **Window sizing polish** (default window fits non-Advanced content; Advanced expands window when toggled) — implemented via `_resize_to_fit`.
- **Frame order correctness in final GIF** — fixed; worker renders newest-to-oldest for efficiency but composes GIF oldest-to-newest chronologically.
- **Cancel process** — works reliably.
- **Keep/delete frames option** — implemented (checkbox in Output section).
