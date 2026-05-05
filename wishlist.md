# World Map Timeline Tool for Minecraft (WMTT4MC) — Wishlist

A backlog of ideas to revisit after core stability and correctness are solid.

## Problems to fix
- ETA is still unreliable

## Rendering speed & efficiency
- Progressive outputs: generate an animated GIF as soon as 2 frames exist, then update/overwrite the GIF each time a new frame completes (so partial results are viewable if a run is cancelled).

## Crop / camera tools
- Visual crop selection from a quick-scan map preview.
- Auto-crop: pick an area matching the target aspect ratio that maximizes "interesting" chunks (eg, highest density of non-empty chunks).
- Frame alignment / no-jump mode:
  - Auto-align frames so the same world coordinates stay registered even as the discovered bounds grow over time (no manual crop required).
  - When a crop box is specified, always render exactly that crop for every frame (even if large areas are blank early on) — currently the image bounds are re-derived from discovered chunks inside the crop box, not pinned to the user's exact coordinates, so jumping can still occur.
- Very advanced: auto-pan/auto-zoom through the timelapse to highlight regions of build activity (detect changes between consecutive frames).

## Color, palettes, and visuals
- Biome tint toggle: Off / Basic / Accurate.
  - Off: treat tintable textures as their default (roughly "plains").
  - Basic: apply a single fixed biome tint (plains) everywhere (faster).
  - Accurate: read biome data per column and apply biome-specific grass/foliage/water tint (slower).
- Flowers rendering toggle (render as full pixel vs treat as transparent) to reduce noise, while still allowing colorful flower fields.
- "Night mode" (advanced): dim all blocks except within a radius of light sources, accounting for light level falloff.

## Higher detail rendering
- "super resolution" mode (eg, 3x3 pixels per block) to show thin structures (fences, rails, flowers/torches as a small mark) based on block orientation/state.

## Output formats & quality
- Include filename overlay on gif for each frame
- look for recognizable date formats in filenames and render with accurate timing with timeline rendered at the bottom
- File size estimator for frames + final output. Warn if estimated size is large (eg >500MB or >10% of free space on output drive).
- Optional output video (MP4/H.264, WebM) in addition to GIF (advanced). Likely requires user-provided `ffmpeg` path; explore bundling alternatives if feasible.




