# World Map Timeline Tool for Minecraft (WMTT4MC) — Wishlist

A backlog of ideas to revisit after core stability and correctness are solid.

## Palette / Rendering problems
- Fix missing blocks and wrong-color issues by aligning with Minecraft in-game map color process as defined in the wiki (https://minecraft.wiki/w/Map_item_format)
- Have default shading be based on wiki documented shading for in-game maps (https://minecraft.wiki/w/Map_item_format)

## Problems to fix
- Takes a long time to stop a job and close the app in the middle of a job
- ETA is still unreliable or takes a long time to calculate.
- ETA is showing number of levels being worked on at once jumping around oddly (present when more than one copy of the app is running at once, might be affecting it?)

## Rendering speed & efficiency
- For a cache refresh/create mode, have it check hashes of the world backup to make sure the world file hasn't changed, then skip that file for cache creation, moving on to look for files that have either updated (hashes don't match) or are new with no caches
- Recovery from interrupted cache process
- Progressive outputs: generate an animated GIF as soon as 2 frames exist, then update/overwrite the GIF each time a new frame completes (so partial results are viewable if a run is cancelled).

## Crop / camera tools
- Condense mode for far-apart regions: collapse very large empty X/Z gaps between occupied areas, and draw divider markers so users can tell where space was removed.
- Auto-pan/auto-zoom through the timelapse to highlight regions of build activity (detect changes between consecutive frames).

## Color, palettes, and visuals
- Biome tint toggle: Off / Basic / Accurate.
  - Off: treat tintable textures as their default (roughly "plains").
  - Basic: apply a single fixed biome tint (plains) everywhere (faster).
  - Accurate: read biome data per column and apply biome-specific grass/foliage/water tint (slower).
- Flowers rendering toggle (render as full pixel vs treat as transparent) to reduce noise, while still allowing colorful flower fields.
- Torch rendering toggle (turn off torches)
- "Night mode" (advanced): dim all blocks except within a radius of light sources, accounting for light level falloff.

## Higher detail rendering
- "super resolution" mode (eg, 3x3 pixels per block) to show thin structures (fences, rails, flowers/torches as a small mark) based on block orientation/state. (would require caches to cache top 2 blocks, not just top block)

## Output formats & quality
- Based on file dates, render with accurate relative timing, with timeline rendered at the bottom
- File size estimator for frames + final output. Warn if estimated size is large (eg >500MB or >10% of free space on output drive).
- Optional output video (MP4/H.264, WebM) in addition to GIF (advanced). Likely requires user-provided `ffmpeg` path; explore bundling alternatives if feasible.




